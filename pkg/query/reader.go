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

// SubjectsFilter selects the relationships needed to enumerate subjects: every
// subject of the given type and relation reachable from any of ResourceIDs.
//
// ResourceIDs is plural so that enumerating from several resources at once —
// a recursion ply, or an arrow draining its left side — costs one query rather
// than one per resource. An empty ResourceIDs applies no resource ID
// constraint, which is how wildcard expansion asks for every resource of the
// type. SubjectRelation drives the ellipsis-vs-non-ellipsis filter.
type SubjectsFilter struct {
	ResourceType     string
	ResourceIDs      []string
	ResourceRelation string
	SubjectType      string
	SubjectRelation  string
	WithCaveats      bool
	WithExpiration   bool
	Page             QueryPage
}

// ResourcesFilter selects the relationships needed to enumerate resources:
// every resource of the given type and relation reachable from any of
// SubjectIDs. It is the subject-axis counterpart of SubjectsFilter; SubjectIDs
// may contain WildcardObjectID for wildcard resource queries.
type ResourcesFilter struct {
	ResourceType     string
	ResourceRelation string
	SubjectType      string
	SubjectIDs       []string
	SubjectRelation  string
	WithCaveats      bool
	WithExpiration   bool
	Page             QueryPage
}

// QueryPage bundles pagination parameters for SubjectsFilter and ResourcesFilter.
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

	// QuerySubjects finds all subject paths for the filter's resources.
	QuerySubjects(ctx context.Context, filter SubjectsFilter) (PathSeq, error)

	// QueryResources finds all resource paths for the filter's subjects.
	QueryResources(ctx context.Context, filter ResourcesFilter) (PathSeq, error)

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

func (r *datalayerQueryDatastoreReader) QuerySubjects(ctx context.Context, subjects SubjectsFilter) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subjects.SubjectType,
				RelationFilter:      buildSubjectRelationFilter(subjects.SubjectRelation),
			},
		},
	}
	// Non-empty fields constrain the query; empty means no constraint on that axis.
	if subjects.ResourceType != "" {
		filter.OptionalResourceType = subjects.ResourceType
	}
	if len(subjects.ResourceIDs) > 0 {
		filter.OptionalResourceIds = subjects.ResourceIDs
	}
	if subjects.ResourceRelation != "" {
		filter.OptionalResourceRelation = subjects.ResourceRelation
	}

	// Choose the query shape based on whether subject filters are present.
	// When subject type/relation are in the SQL WHERE clause, the filter has a gap
	// in the PK columns (subject_id is not filtered but subject_type and subject_relation
	// are), which can cause CockroachDB to reject the forced pk_relation_tuple hint.
	// In that case, use Varying so the datastore picks an index based on actual filter columns.
	// When there are no subject filters, AllSubjectsForResources is correct and matches
	// the traditional dispatch path.
	shape := queryshape.AllSubjectsForResources
	if subjects.SubjectType != "" {
		shape = queryshape.Varying
	}
	queryOpts := []options.QueryOptionsOption{
		options.WithSkipCaveats(!subjects.WithCaveats),
		options.WithSkipExpiration(!subjects.WithExpiration),
		options.WithQueryShape(shape),
	}
	if subjects.Page.Limit != nil {
		queryOpts = append(queryOpts,
			options.WithLimit(subjects.Page.Limit),
			options.WithSort(options.ChooseEfficient),
		)
	}
	if subjects.Page.Cursor != nil {
		queryOpts = append(queryOpts, options.WithAfter(options.ToCursor(*subjects.Page.Cursor)))
	}

	relIter, err := r.inner.QueryRelationships(ctx, filter, queryOpts...)
	if err != nil {
		return nil, err
	}
	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *datalayerQueryDatastoreReader) QueryResources(ctx context.Context, resources ResourcesFilter) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     resources.ResourceType,
		OptionalResourceRelation: resources.ResourceRelation,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: resources.SubjectType,
				OptionalSubjectIds:  resources.SubjectIDs,
				RelationFilter:      buildSubjectRelationFilter(resources.SubjectRelation),
			},
		},
	}

	queryOpts := []options.QueryOptionsOption{
		options.WithSkipCaveats(!resources.WithCaveats),
		options.WithSkipExpiration(!resources.WithExpiration),
		options.WithQueryShape(queryshape.MatchingResourcesForSubject),
	}
	if resources.Page.Limit != nil {
		queryOpts = append(queryOpts,
			options.WithLimit(resources.Page.Limit),
			options.WithSort(options.ChooseEfficient),
		)
	}
	if resources.Page.Cursor != nil {
		queryOpts = append(queryOpts, options.WithAfter(options.ToCursor(*resources.Page.Cursor)))
	}

	relIter, err := r.inner.QueryRelationships(ctx, filter, queryOpts...)
	if err != nil {
		return nil, err
	}
	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
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
