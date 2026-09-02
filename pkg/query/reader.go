package query

import (
	"context"
	"iter"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/tuple"
)

// WildcardObjectID is the subject ID representing a public wildcard ("*").
const WildcardObjectID = tuple.PublicWildcard

// limitOne is used for existence-probe queries that only need to know if
// at least one row exists.
var limitOne uint64 = 1

// CheckFilter selects the relationships a check needs: those connecting any of
// ResourceIDs to any of SubjectIDs, under a fixed relation on each side.
//
// Both axes are slices so that CheckManyResources (many resources, one subject)
// and CheckManySubjects (one resource, many subjects) collapse into a single
// datastore query rather than one per element. A plain Check passes one ID on
// each side. SubjectIDs may contain WildcardObjectID for wildcard checks.
type CheckFilter struct {
	ResourceType     string
	ResourceIDs      []string
	ResourceRelation string
	SubjectType      string
	SubjectIDs       []string
	SubjectRelation  string
	WithCaveats      bool
	WithExpiration   bool
}

// QueryPage bundles pagination parameters for QuerySubjects and QueryResources.
type QueryPage struct {
	Limit  *uint64
	Cursor *tuple.Relationship
}

// QueryDatastoreReader is the minimal datastore interface used by pkg/query.
// It exposes only the four logical operations actually performed by this package,
// returning PathSeq values directly so callers never touch raw relationship iterators.
type QueryDatastoreReader interface {
	// CheckRelationships finds paths matching the given filter. Both the
	// resource and subject axes are plural so that a batched check reaches the
	// datastore as a single query; a scalar check passes single-element slices.
	CheckRelationships(ctx context.Context, filter CheckFilter) (PathSeq, error)

	// QuerySubjects finds all subject paths for a resource.
	// If resource.ObjectID is empty, no resource ID filter is applied (wildcard expansion).
	// subjectType.Subrelation drives the ellipsis-vs-non-ellipsis filter.
	QuerySubjects(
		ctx context.Context,
		resource Object,
		resourceRelation string,
		subjectType ObjectType,
		withCaveats, withExpiration bool,
		page QueryPage,
	) (PathSeq, error)

	// QueryResources finds all resource paths for a subject.
	// subject.ObjectID may be WildcardObjectID for wildcard resource queries.
	QueryResources(
		ctx context.Context,
		resourceType string,
		resourceRelation string,
		subject ObjectAndRelation,
		withCaveats, withExpiration bool,
		page QueryPage,
	) (PathSeq, error)

	// SubjectExistsAsRelationship is an existence probe used by AliasIterator.
	// It includes expired relationships and returns true if any relationship
	// has the given subject with the specified non-ellipsis relation.
	SubjectExistsAsRelationship(
		ctx context.Context,
		subject Object,
		nonEllipsisRelation string,
	) (bool, error)

	// LookupCaveatDefinition fetches a single caveat definition by name.
	// Implementations are expected to cache results.
	LookupCaveatDefinition(
		ctx context.Context,
		name string,
	) (datastore.CaveatDefinition, error)
}

// NewQueryDatastoreReader wraps a datalayer.RevisionedReader as a QueryDatastoreReader.
func NewQueryDatastoreReader(r datalayer.RevisionedReader) QueryDatastoreReader {
	return &datalayerQueryDatastoreReader{inner: r}
}

type datalayerQueryDatastoreReader struct {
	inner datalayer.RevisionedReader
}

// convertRelationSeqToPathSeq converts an iter.Seq2[tuple.Relationship, error] from
// the datastore into a PathSeq by transforming each Relationship into a Path.
func convertRelationSeqToPathSeq(relSeq iter.Seq2[tuple.Relationship, error]) PathSeq {
	return func(yield func(*Path, error) bool) {
		for rel, err := range relSeq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !yield(FromRelationship(rel), nil) {
				return
			}
		}
	}
}

// buildSubjectRelationFilter returns the appropriate SubjectRelationFilter for a
// given subrelation string: ellipsis → WithEllipsisRelation, otherwise → WithNonEllipsisRelation.
func buildSubjectRelationFilter(subrelation string) datastore.SubjectRelationFilter {
	if subrelation == tuple.Ellipsis {
		return datastore.SubjectRelationFilter{}.WithEllipsisRelation()
	}
	return datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(subrelation)
}

func (r *datalayerQueryDatastoreReader) CheckRelationships(ctx context.Context, check CheckFilter) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     check.ResourceType,
		OptionalResourceIds:      check.ResourceIDs,
		OptionalResourceRelation: check.ResourceRelation,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: check.SubjectType,
				OptionalSubjectIds:  check.SubjectIDs,
				RelationFilter:      buildSubjectRelationFilter(check.SubjectRelation),
			},
		},
	}

	relIter, err := r.inner.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(!check.WithCaveats),
		options.WithSkipExpiration(!check.WithExpiration),
		options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects),
	)
	if err != nil {
		return nil, err
	}
	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *datalayerQueryDatastoreReader) QuerySubjects(
	ctx context.Context,
	resource Object,
	resourceRelation string,
	subjectType ObjectType,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subjectType.Type,
				RelationFilter:      buildSubjectRelationFilter(subjectType.Subrelation),
			},
		},
	}
	// Non-empty fields constrain the query; empty means no constraint on that axis.
	if resource.ObjectType != "" {
		filter.OptionalResourceType = resource.ObjectType
	}
	if resource.ObjectID != "" {
		filter.OptionalResourceIds = []string{resource.ObjectID}
	}
	if resourceRelation != "" {
		filter.OptionalResourceRelation = resourceRelation
	}

	// Choose the query shape based on whether subject filters are present.
	// When subject type/relation are in the SQL WHERE clause, the filter has a gap
	// in the PK columns (subject_id is not filtered but subject_type and subject_relation
	// are), which can cause CockroachDB to reject the forced pk_relation_tuple hint.
	// In that case, use Varying so the datastore picks an index based on actual filter columns.
	// When there are no subject filters, AllSubjectsForResources is correct and matches
	// the traditional dispatch path.
	shape := queryshape.AllSubjectsForResources
	if subjectType.Type != "" {
		shape = queryshape.Varying
	}
	queryOpts := []options.QueryOptionsOption{
		options.WithSkipCaveats(!withCaveats),
		options.WithSkipExpiration(!withExpiration),
		options.WithQueryShape(shape),
	}
	if page.Limit != nil {
		queryOpts = append(queryOpts,
			options.WithLimit(page.Limit),
			options.WithSort(options.ChooseEfficient),
		)
	}
	if page.Cursor != nil {
		queryOpts = append(queryOpts, options.WithAfter(options.ToCursor(*page.Cursor)))
	}

	relIter, err := r.inner.QueryRelationships(ctx, filter, queryOpts...)
	if err != nil {
		return nil, err
	}
	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *datalayerQueryDatastoreReader) QueryResources(
	ctx context.Context,
	resourceType string,
	resourceRelation string,
	subject ObjectAndRelation,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     resourceType,
		OptionalResourceRelation: resourceRelation,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subject.ObjectType,
				OptionalSubjectIds:  []string{subject.ObjectID},
				RelationFilter:      buildSubjectRelationFilter(subject.Relation),
			},
		},
	}

	queryOpts := []options.QueryOptionsOption{
		options.WithSkipCaveats(!withCaveats),
		options.WithSkipExpiration(!withExpiration),
		options.WithQueryShape(queryshape.MatchingResourcesForSubject),
	}
	if page.Limit != nil {
		queryOpts = append(queryOpts,
			options.WithLimit(page.Limit),
			options.WithSort(options.ChooseEfficient),
		)
	}
	if page.Cursor != nil {
		queryOpts = append(queryOpts, options.WithAfter(options.ToCursor(*page.Cursor)))
	}

	relIter, err := r.inner.QueryRelationships(ctx, filter, queryOpts...)
	if err != nil {
		return nil, err
	}
	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *datalayerQueryDatastoreReader) SubjectExistsAsRelationship(
	ctx context.Context,
	subject Object,
	nonEllipsisRelation string,
) (bool, error) {
	filter := datastore.RelationshipsFilter{
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subject.ObjectType,
				OptionalSubjectIds:  []string{subject.ObjectID},
				RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(nonEllipsisRelation),
			},
		},
		OptionalExpirationOption: datastore.ExpirationFilterOptionNone,
	}

	// The filter constrains subject type, subject ID and subject relation with
	// no resource constraint at all, which matches none of the specific query
	// shapes; Varying lets the datastore pick an index from the columns actually
	// filtered rather than forcing one that does not fit.
	relIter, err := r.inner.QueryRelationships(ctx, filter,
		options.WithLimit(&limitOne),
		options.WithSkipExpiration(true),
		options.WithQueryShape(queryshape.Varying),
	)
	if err != nil {
		return false, err
	}

	for _, err := range relIter {
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *datalayerQueryDatastoreReader) LookupCaveatDefinition(
	ctx context.Context,
	name string,
) (datastore.CaveatDefinition, error) {
	sr, err := r.inner.ReadSchema(ctx)
	if err != nil {
		return nil, err
	}
	defs, err := sr.LookupCaveatDefinitionsByNames(ctx, []string{name})
	if err != nil {
		return nil, err
	}
	def, ok := defs[name]
	if !ok {
		return nil, datastore.NewCaveatNameNotFoundErr(name)
	}
	return def, nil
}
