package query

import (
	"errors"
	"fmt"
	"io"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

func init() {
	MustRegisterIterator(IteratorSpec{
		Type: DatastoreIteratorType,
		Name: "Datastore",
		ConstructWithArgs: func(args *IteratorArgs, _ []Iterator, key CanonicalKey) (Iterator, error) {
			if args == nil || args.Relation == nil {
				return nil, errors.New("DatastoreIterator requires Relation in Args")
			}
			ds := NewDatastoreIterator(args.Relation)
			ds.canonicalKey = key
			return ds, nil
		},
		Deserialize: deserializeDatastore,
	})
}

// DatastoreIterator is a common leaf iterator. It represents the set of all
// relationships of the given schema.BaseRelation, ie, relations that have a
// known resource and subject type and may contain caveats or expiration.
//
// The DatastoreIterator, being the leaf, generates this set by calling the datastore.
type DatastoreIterator struct {
	base         *schema.BaseRelation
	canonicalKey CanonicalKey
}

var _ Iterator = &DatastoreIterator{}

func NewDatastoreIterator(base *schema.BaseRelation) *DatastoreIterator {
	return &DatastoreIterator{
		base: base,
	}
}

func (r *DatastoreIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// For subrelations, we need to allow type mismatches because the subrelation might bridge different types
	// For example, group:member -> group:member should find group:everyone#member@group:engineering#member
	// and then that relationship should be used by the Arrow to check group:engineering#member for user subjects
	// However, wildcard relations and ellipsis relations should always enforce strict type checking
	// Ellipsis (...) means "any relation on the same type", not "bridging to a different type"
	if subject.ObjectType != r.base.Type() && r.base.Subrelation() != "" && r.base.Subrelation() != tuple.Ellipsis && !r.base.Wildcard() {
		// For non-wildcard, non-ellipsis subrelations, we proceed with the query even if types don't match
		// This allows finding intermediate relationships that bridge type gaps
		if ctx.shouldTrace() {
			ctx.TraceStep(r, "subject type %s doesn't match base type %s, but proceeding due to subrelation %s",
				subject.ObjectType, r.base.Type(), r.base.Subrelation())
		}
	} else if subject.ObjectType != r.base.Type() {
		// For non-subrelations, ellipsis, and all wildcard relations, strict type checking applies
		if ctx.shouldTrace() {
			ctx.TraceStep(r, "subject type %s doesn't match base type %s, returning empty", subject.ObjectType, r.base.Type())
		}
		return nil, nil
	}

	if r.base.Wildcard() {
		return r.checkWildcardImpl(ctx, resource, subject)
	}
	return r.checkNormalImpl(ctx, resource, subject)
}

// checkFilter builds the datastore filter for a check over the given resource
// and subject IDs. Both axes are plural: a scalar check passes one ID on each
// side, a batched check passes the whole set so it costs one query rather than
// one per element.
func (r *DatastoreIterator) checkFilter(resourceIDs []string, subjectType string, subjectIDs []string, subjectRelation string) CheckFilter {
	return CheckFilter{
		ResourceType:     r.base.DefinitionName(),
		ResourceIDs:      resourceIDs,
		ResourceRelation: r.base.RelationName(),
		SubjectType:      subjectType,
		SubjectIDs:       subjectIDs,
		SubjectRelation:  subjectRelation,
		WithCaveats:      r.base.Caveat() != "",
		WithExpiration:   r.base.Expiration(),
	}
}

// subjectTypeAllowed mirrors the type gate in CheckImpl: a subject whose type
// does not match the base relation's type is rejected without a query, unless
// the base relation carries a concrete subrelation, which may legitimately
// bridge to a different type.
func (r *DatastoreIterator) subjectTypeAllowed(subjectType string) bool {
	if subjectType == r.base.Type() {
		return true
	}
	return r.base.Subrelation() != "" && r.base.Subrelation() != tuple.Ellipsis && !r.base.Wildcard()
}

func (r *DatastoreIterator) checkNormalImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(r, "querying datastore for %s:%s with resource=%s:%s", r.base.Type(), r.base.RelationName(), resource.ObjectType, resource.ObjectID)
	}

	pathSeq, err := ctx.Reader.CheckRelationships(ctx,
		r.checkFilter([]string{resource.ObjectID}, subject.ObjectType, []string{subject.ObjectID}, subject.Relation))
	if err != nil {
		return nil, err
	}

	// Collect results and return the first (there is at most one for a single resource).
	// Eagerly collecting also terminates the database query immediately.
	paths, err := CollectAll(pathSeq)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}
	return paths[0], nil
}

func (r *DatastoreIterator) checkWildcardImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// Invariant: wildcard subjects in the datastore are always stored with the ellipsis
	// relation. The "*" is only ever an ObjectID; "type:*#relation" is syntactically
	// invalid and cannot be written. Any caller passing a non-ellipsis relation here
	// would cause us to query with the wrong relation filter and return a false negative.
	if subject.Relation != tuple.Ellipsis {
		return nil, spiceerrors.MustBugf("checkWildcardImpl called with non-ellipsis subject relation %q for subject %s:%s; wildcard subjects are always stored with ellipsis relation", subject.Relation, subject.ObjectType, subject.ObjectID)
	}

	// Query the datastore for wildcard relationships (subject ObjectID = "*")
	wildcardSubject := ObjectAndRelation{
		ObjectType: subject.ObjectType,
		ObjectID:   WildcardObjectID,
		Relation:   tuple.Ellipsis,
	}

	pathSeq, err := ctx.Reader.CheckRelationships(ctx,
		r.checkFilter([]string{resource.ObjectID}, wildcardSubject.ObjectType, []string{wildcardSubject.ObjectID}, wildcardSubject.Relation))
	if err != nil {
		return nil, err
	}

	// Rewrite subjects from wildcard back to the actual subject, collect, return first.
	pathSeq = RewriteSubject(pathSeq, subject)
	paths, err := CollectAll(pathSeq)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}
	return paths[0], nil
}

// CheckManySubjectsImpl answers a batch of subjects against one resource with a
// single datastore query, by putting every subject ID into the filter instead
// of issuing one query per subject.
//
// Subjects are grouped by (type, relation) because those two fields go into the
// query filter rather than the ID list; in practice a batch arriving from an
// arrow is homogeneous and forms a single group. Subjects whose type the base
// relation cannot accept are left nil without a query, matching CheckImpl.
func (r *DatastoreIterator) CheckManySubjectsImpl(ctx *Context, resource Object, subjects []ObjectAndRelation) ([]*Path, error) {
	out := make([]*Path, len(subjects))

	// Wildcard checks rewrite the subject per element, so they stay on the
	// scalar path.
	if r.base.Wildcard() {
		for i, subject := range subjects {
			path, err := r.CheckImpl(ctx, resource, subject)
			if err != nil {
				return nil, err
			}
			out[i] = path
		}
		return out, nil
	}

	type subjectGroup struct{ objectType, relation string }
	groups := make(map[subjectGroup][]int, 1)
	for i, subject := range subjects {
		if !r.subjectTypeAllowed(subject.ObjectType) {
			continue
		}
		key := subjectGroup{subject.ObjectType, subject.Relation}
		groups[key] = append(groups[key], i)
	}

	for key, indexes := range groups {
		ids := make([]string, 0, len(indexes))
		seen := make(map[string]struct{}, len(indexes))
		for _, i := range indexes {
			if _, ok := seen[subjects[i].ObjectID]; ok {
				continue
			}
			seen[subjects[i].ObjectID] = struct{}{}
			ids = append(ids, subjects[i].ObjectID)
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(r, "batched datastore check for %s:%s with resource=%s:%s and %d subjects",
				r.base.Type(), r.base.RelationName(), resource.ObjectType, resource.ObjectID, len(ids))
		}

		pathSeq, err := ctx.Reader.CheckRelationships(ctx,
			r.checkFilter([]string{resource.ObjectID}, key.objectType, ids, key.relation))
		if err != nil {
			return nil, err
		}
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}

		bySubject := make(map[ObjectAndRelation]*Path, len(paths))
		for _, path := range paths {
			bySubject[path.Subject] = path
		}
		for _, i := range indexes {
			out[i] = bySubject[subjects[i]]
		}
	}

	return out, nil
}

// CheckManyResourcesImpl answers a batch of resources against one subject with
// a single datastore query, the resource-axis counterpart of
// CheckManySubjectsImpl.
func (r *DatastoreIterator) CheckManyResourcesImpl(ctx *Context, resources []Object, subject ObjectAndRelation) ([]*Path, error) {
	out := make([]*Path, len(resources))

	// Wildcard checks rewrite the subject per element, so they stay on the
	// scalar path.
	if r.base.Wildcard() {
		for i, resource := range resources {
			path, err := r.CheckImpl(ctx, resource, subject)
			if err != nil {
				return nil, err
			}
			out[i] = path
		}
		return out, nil
	}

	// One subject for the whole batch: if its type is not acceptable, no
	// resource can match and no query is needed.
	if !r.subjectTypeAllowed(subject.ObjectType) {
		return out, nil
	}

	ids := make([]string, 0, len(resources))
	seen := make(map[string]struct{}, len(resources))
	for _, resource := range resources {
		if _, ok := seen[resource.ObjectID]; ok {
			continue
		}
		seen[resource.ObjectID] = struct{}{}
		ids = append(ids, resource.ObjectID)
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(r, "batched datastore check for %s:%s with %d resources and subject=%s:%s",
			r.base.Type(), r.base.RelationName(), len(ids), subject.ObjectType, subject.ObjectID)
	}

	pathSeq, err := ctx.Reader.CheckRelationships(ctx,
		r.checkFilter(ids, subject.ObjectType, []string{subject.ObjectID}, subject.Relation))
	if err != nil {
		return nil, err
	}
	paths, err := CollectAll(pathSeq)
	if err != nil {
		return nil, err
	}

	byResource := make(map[Object]*Path, len(paths))
	for _, path := range paths {
		byResource[path.Resource] = path
	}
	for i, resource := range resources {
		out[i] = byResource[resource]
	}

	return out, nil
}

// objectIDs collects the IDs of the given objects, skipping empties and
// duplicates. An empty result means "no resource ID constraint", which is how
// wildcard expansion asks for every resource of the type.
func objectIDs(objects []Object) []string {
	ids := make([]string, 0, len(objects))
	seen := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		if object.ObjectID == "" {
			continue
		}
		if _, ok := seen[object.ObjectID]; ok {
			continue
		}
		seen[object.ObjectID] = struct{}{}
		ids = append(ids, object.ObjectID)
	}
	return ids
}

// subjectsFilter builds the datastore filter for enumerating subjects of the
// given resources. resourceIDs is plural so a batch of starting points costs one
// query; an empty slice applies no resource ID constraint.
func (r *DatastoreIterator) subjectsFilter(resourceType string, resourceIDs []string, page QueryPage) SubjectsFilter {
	return SubjectsFilter{
		ResourceType:     resourceType,
		ResourceIDs:      resourceIDs,
		ResourceRelation: r.base.RelationName(),
		SubjectType:      r.base.Type(),
		SubjectRelation:  r.base.Subrelation(),
		WithCaveats:      r.base.Caveat() != "",
		WithExpiration:   r.base.Expiration(),
		Page:             page,
	}
}

// resourcesFilter builds the datastore filter for enumerating resources of the
// given subjects. subjectIDs is plural for the same reason.
func (r *DatastoreIterator) resourcesFilter(subjectType string, subjectIDs []string, subjectRelation string, page QueryPage) ResourcesFilter {
	return ResourcesFilter{
		ResourceType:     r.base.DefinitionName(),
		ResourceRelation: r.base.RelationName(),
		SubjectType:      subjectType,
		SubjectIDs:       subjectIDs,
		SubjectRelation:  subjectRelation,
		WithCaveats:      r.base.Caveat() != "",
		WithExpiration:   r.base.Expiration(),
		Page:             page,
	}
}

func (r *DatastoreIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	if r.base.Wildcard() {
		return r.iterSubjectsWildcardImpl(ctx, resource)
	}
	return r.iterSubjectsNormalImpl(ctx, resource.ObjectType, objectIDs([]Object{resource}))
}

// IterSubjectsForResourcesImpl enumerates the subjects of every resource in one
// query by putting the whole ID set into the filter.
//
// Pagination keeps a single cursor per iterator, which cannot be shared across
// a batch of starting points, so a paginated call — and a wildcard base, which
// rewrites per resource — walks one resource at a time.
func (r *DatastoreIterator) IterSubjectsForResourcesImpl(ctx *Context, resources []Object, filterSubjectType ObjectType) (PathSeq, error) {
	if r.base.Wildcard() || ctx.PaginationLimit != nil {
		return iterSubjectsPerResource(ctx, r, resources, filterSubjectType)
	}
	if len(resources) == 0 {
		return EmptyPathSeq(), nil
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(r, "batched datastore subjects query for %s:%s across %d resources",
			r.base.DefinitionName(), r.base.RelationName(), len(resources))
	}
	return r.iterSubjectsNormalImpl(ctx, resources[0].ObjectType, objectIDs(resources))
}

func (r *DatastoreIterator) iterSubjectsNormalImpl(ctx *Context, resourceType string, resourceIDs []string) (PathSeq, error) {
	// If pagination is not configured, do the simple eager collection
	if ctx.PaginationLimit == nil {
		pathSeq, err := ctx.Reader.QuerySubjects(ctx, r.subjectsFilter(resourceType, resourceIDs, QueryPage{}))
		if err != nil {
			return nil, err
		}

		// Eagerly collect (wildcards propagate through the tree and are stripped at the top level)
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}
		return PathSeqFromSlice(paths), nil
	}

	// Pagination is configured - return a PathSeq that fetches pages as needed
	return func(yield func(*Path, error) bool) {
		iteratorID := fmt.Sprintf("%016x:iter_subjects", r.CanonicalKey().Hash())
		cursor := ctx.GetPaginationCursor(iteratorID)

		for {
			pathSeq, err := ctx.Reader.QuerySubjects(ctx,
				r.subjectsFilter(resourceType, resourceIDs, QueryPage{Limit: ctx.PaginationLimit, Cursor: cursor}))
			if err != nil {
				yield(nil, err)
				return
			}

			paths, err := CollectAll(pathSeq)
			if err != nil {
				yield(nil, err)
				return
			}

			if len(paths) == 0 {
				return
			}

			lastPath := paths[len(paths)-1]
			if rel, err := lastPath.ToRelationship(); err == nil {
				cursor = &rel
				ctx.SetPaginationCursor(iteratorID, cursor)
			}

			for _, path := range paths {
				if !yield(path, nil) {
					return
				}
			}

			if uint64(len(paths)) < *ctx.PaginationLimit {
				return
			}
		}
	}, nil
}

func (r *DatastoreIterator) iterSubjectsWildcardImpl(ctx *Context, resource Object) (PathSeq, error) {
	// When a relation contains a wildcard (e.g., user:*[caveat]), it means "all subjects of that
	// type are (conditionally) in this set". Rather than enumerating every concrete subject of
	// that type across the entire store, we return the wildcard subject itself (user:*) as the
	// found path. This matches the traditional LookupSubjects behavior and avoids a degenerate
	// store-wide query with no resource filters.
	//
	// The IntersectionIterator and ExclusionIterator have wildcard-aware set operations that
	// handle the expansion of wildcards when combined with concrete subjects from other branches.

	wildcardSubject := ObjectAndRelation{
		ObjectType: r.base.Type(),
		ObjectID:   WildcardObjectID,
		Relation:   r.base.Subrelation(),
	}

	return ctx.Reader.CheckRelationships(ctx,
		r.checkFilter([]string{resource.ObjectID}, wildcardSubject.ObjectType, []string{wildcardSubject.ObjectID}, wildcardSubject.Relation))
}

// IterResourcesForSubjectsImpl enumerates the resources of every subject in one
// query by putting the whole ID set into the filter.
//
// As with IterSubjectsForResourcesImpl, a paginated call or a wildcard base
// walks one subject at a time. Subjects are grouped by relation because that
// field goes into the filter rather than the ID list, and subjects the base
// relation cannot accept are dropped without a query.
func (r *DatastoreIterator) IterResourcesForSubjectsImpl(ctx *Context, subjects []ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	if r.base.Wildcard() || ctx.PaginationLimit != nil {
		return iterResourcesPerSubject(ctx, r, subjects, filterResourceType)
	}

	byRelation := make(map[string][]string, 1)
	relations := make([]string, 0, 1)
	for _, subject := range subjects {
		// Same gates as IterResourcesImpl: a mismatched type or relation cannot
		// produce a row, so it need not reach the datastore.
		if subject.ObjectType != r.base.Type() || subject.Relation != r.base.Subrelation() {
			continue
		}
		if _, ok := byRelation[subject.Relation]; !ok {
			relations = append(relations, subject.Relation)
		}
		byRelation[subject.Relation] = append(byRelation[subject.Relation], subject.ObjectID)
	}
	if len(relations) == 0 {
		return EmptyPathSeq(), nil
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(r, "batched datastore resources query for %s:%s across %d subjects",
			r.base.DefinitionName(), r.base.RelationName(), len(subjects))
	}

	return func(yield func(*Path, error) bool) {
		for _, relation := range relations {
			pathSeq, err := r.iterResourcesNormalImpl(ctx, r.base.Type(), dedupeStrings(byRelation[relation]), relation)
			if err != nil {
				yield(nil, err)
				return
			}
			for path, err := range pathSeq {
				if !yield(path, err) {
					return
				}
			}
		}
	}, nil
}

// dedupeStrings returns ids with duplicates removed, preserving order.
func dedupeStrings(ids []string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func (r *DatastoreIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// If the types don't match, we don't even have to go to the datastore.
	if subject.ObjectType != r.base.Type() {
		return EmptyPathSeq(), nil
	}

	// Handle wildcards first - they don't have subrelations and match any query relation
	if r.base.Wildcard() {
		return r.iterResourcesWildcardImpl(ctx, subject)
	}

	// An empty Relation is always a bug in the caller: it must be either tuple.Ellipsis
	// for direct membership or a specific subrelation string.
	if subject.Relation == "" {
		return nil, spiceerrors.MustBugf("IterResources called with empty subject.Relation for %s:%s; caller must use tuple.Ellipsis or a specific subrelation", subject.ObjectType, subject.ObjectID)
	}

	// Check if subject relation matches what this iterator expects.
	// When the schema uses ellipsis ("..."), callers should pass tuple.Ellipsis — the
	// MustBugf above ensures "" never reaches here — but we match on the schema value directly.
	if r.base.Subrelation() != subject.Relation {
		return EmptyPathSeq(), nil
	}

	return r.iterResourcesNormalImpl(ctx, subject.ObjectType, []string{subject.ObjectID}, subject.Relation)
}

func (r *DatastoreIterator) iterResourcesNormalImpl(ctx *Context, subjectType string, subjectIDs []string, subjectRelation string) (PathSeq, error) {
	if ctx.PaginationLimit == nil {
		pathSeq, err := ctx.Reader.QueryResources(ctx,
			r.resourcesFilter(subjectType, subjectIDs, subjectRelation, QueryPage{}))
		if err != nil {
			return nil, err
		}

		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}
		return PathSeqFromSlice(paths), nil
	}

	return func(yield func(*Path, error) bool) {
		iteratorID := fmt.Sprintf("%016x:iter_resources", r.CanonicalKey().Hash())
		cursor := ctx.GetPaginationCursor(iteratorID)

		for {
			pathSeq, err := ctx.Reader.QueryResources(ctx,
				r.resourcesFilter(subjectType, subjectIDs, subjectRelation, QueryPage{Limit: ctx.PaginationLimit, Cursor: cursor}))
			if err != nil {
				yield(nil, err)
				return
			}

			paths, err := CollectAll(pathSeq)
			if err != nil {
				yield(nil, err)
				return
			}

			if len(paths) == 0 {
				return
			}

			lastPath := paths[len(paths)-1]
			if rel, err := lastPath.ToRelationship(); err == nil {
				cursor = &rel
				ctx.SetPaginationCursor(iteratorID, cursor)
			}

			for _, path := range paths {
				if !yield(path, nil) {
					return
				}
			}

			if uint64(len(paths)) < *ctx.PaginationLimit {
				return
			}
		}
	}, nil
}

func (r *DatastoreIterator) iterResourcesWildcardImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	// Invariant: wildcard subjects in the datastore are always stored with the ellipsis
	// relation. The "*" is only ever an ObjectID; "type:*#relation" is syntactically
	// invalid and cannot be written. Any caller passing a non-ellipsis relation here
	// would cause us to query with the wrong relation filter and return a false negative.
	if subject.Relation != tuple.Ellipsis {
		return nil, spiceerrors.MustBugf("iterResourcesWildcardImpl called with non-ellipsis subject relation %q for subject %s:%s; wildcard subjects are always stored with ellipsis relation", subject.Relation, subject.ObjectType, subject.ObjectID)
	}

	wildcardSubject := ObjectAndRelation{
		ObjectType: subject.ObjectType,
		ObjectID:   WildcardObjectID,
		Relation:   tuple.Ellipsis,
	}

	if ctx.PaginationLimit == nil {
		pathSeq, err := ctx.Reader.QueryResources(ctx,
			r.resourcesFilter(wildcardSubject.ObjectType, []string{wildcardSubject.ObjectID}, wildcardSubject.Relation, QueryPage{}))
		if err != nil {
			return nil, err
		}

		pathSeq = RewriteSubject(pathSeq, subject)
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}
		return PathSeqFromSlice(paths), nil
	}

	return func(yield func(*Path, error) bool) {
		iteratorID := fmt.Sprintf("%016x:iter_resources_wildcard", r.CanonicalKey().Hash())
		cursor := ctx.GetPaginationCursor(iteratorID)

		for {
			pathSeq, err := ctx.Reader.QueryResources(ctx,
				r.resourcesFilter(wildcardSubject.ObjectType, []string{wildcardSubject.ObjectID}, wildcardSubject.Relation, QueryPage{Limit: ctx.PaginationLimit, Cursor: cursor}))
			if err != nil {
				yield(nil, err)
				return
			}

			pathSeq = RewriteSubject(pathSeq, subject)
			paths, err := CollectAll(pathSeq)
			if err != nil {
				yield(nil, err)
				return
			}

			if len(paths) == 0 {
				return
			}

			lastPath := paths[len(paths)-1]
			if rel, err := lastPath.ToRelationship(); err == nil {
				cursor = &rel
				ctx.SetPaginationCursor(iteratorID, cursor)
			}

			for _, path := range paths {
				if !yield(path, nil) {
					return
				}
			}

			if uint64(len(paths)) < *ctx.PaginationLimit {
				return
			}
		}
	}, nil
}

func (r *DatastoreIterator) Clone() Iterator {
	return &DatastoreIterator{
		canonicalKey: r.canonicalKey,
		base:         r.base,
	}
}

func (r *DatastoreIterator) Explain() Explain {
	relationName := r.base.Subrelation()
	if r.base.Wildcard() {
		relationName = "*"
	}
	return Explain{
		Info: fmt.Sprintf("Datastore(%s:%s -> %s:%s, caveat: %v, expiration: %v)",
			r.base.DefinitionName(), r.base.RelationName(), r.base.Type(), relationName,
			r.base.Caveat() != "", r.base.Expiration()),
	}
}

func (r *DatastoreIterator) Subiterators() []Iterator {
	return nil
}

func (r *DatastoreIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return nil, spiceerrors.MustBugf("Trying to replace a leaf DatastoreIterator's subiterators")
}

func (r *DatastoreIterator) CanonicalKey() CanonicalKey {
	return r.canonicalKey
}

func (r *DatastoreIterator) ResourceType() ([]ObjectType, error) {
	return []ObjectType{{
		Type:        r.base.DefinitionName(),
		Subrelation: tuple.Ellipsis,
	}}, nil
}

func (r *DatastoreIterator) SubjectTypes() ([]ObjectType, error) {
	// For wildcards, return the base type with no subrelation
	if r.base.Wildcard() {
		return []ObjectType{{
			Type:        r.base.Type(),
			Subrelation: "",
		}}, nil
	}

	// For ellipsis, preserve the ellipsis subrelation so callers that construct
	// ObjectAndRelation values from SubjectTypes get the correct relation to query with.
	if r.base.Subrelation() == tuple.Ellipsis {
		return []ObjectType{{
			Type:        r.base.Type(),
			Subrelation: tuple.Ellipsis,
		}}, nil
	}

	// For regular subrelations, return the specific type and subrelation
	return []ObjectType{{
		Type:        r.base.Type(),
		Subrelation: r.base.Subrelation(),
	}}, nil
}

const (
	dsFlagCaveat = iota
	dsFlagExpiration
	dsFlagWildcard
)

func (r *DatastoreIterator) Serialize(w io.Writer) error {
	return SerializeWithHeader(w, DatastoreIteratorType, r.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, dsFlagCaveat, r.base.Caveat() != "")
		setFlag(&flags, dsFlagExpiration, r.base.Expiration())
		setFlag(&flags, dsFlagWildcard, r.base.Wildcard())
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		// Always-present identifying fields.
		if err := writeString(buf, r.base.DefinitionName()); err != nil {
			return err
		}
		if err := writeString(buf, r.base.RelationName()); err != nil {
			return err
		}
		if err := writeString(buf, r.base.Type()); err != nil {
			return err
		}
		if err := writeString(buf, r.base.Subrelation()); err != nil {
			return err
		}
		if hasFlag(flags, dsFlagCaveat) {
			if err := writeString(buf, r.base.Caveat()); err != nil {
				return err
			}
		}
		return nil
	})
}

func deserializeDatastore(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	if dctx == nil || dctx.Schema == nil {
		return nil, errors.New("DatastoreIterator deserialize requires DeserializeContext with Schema")
	}
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("datastore flags: %w", err)
	}
	defName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("datastore def: %w", err)
	}
	relName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("datastore rel: %w", err)
	}
	subjectType, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("datastore subjectType: %w", err)
	}
	subrelation, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("datastore subrelation: %w", err)
	}
	var caveat string
	if hasFlag(flags, dsFlagCaveat) {
		if caveat, err = readString(br); err != nil {
			return nil, fmt.Errorf("datastore caveat: %w", err)
		}
	}
	base, err := dctx.Schema.ResolveBaseRelation(
		defName, relName, subjectType, subrelation, caveat,
		hasFlag(flags, dsFlagExpiration), hasFlag(flags, dsFlagWildcard),
	)
	if err != nil {
		return nil, fmt.Errorf("datastore: %w", err)
	}
	ds := NewDatastoreIterator(base)
	ds.canonicalKey = key
	return ds, nil
}
