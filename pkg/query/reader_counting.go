package query

import (
	"context"
	"strings"
	"sync"

	"github.com/authzed/spicedb/pkg/datastore"
)

// CountingReader is a QueryDatastoreReader shim that records how many datastore
// round-trips a query execution performs. It delegates all logic to an inner
// reader.
//
// Round-trips — not wall-clock — are the metric that matters for workloads
// bounded by datastore connections, where the pool is the scarce resource. Use
// this in benchmarks to report queries-per-operation alongside ns/op, so a
// change that trades one query for many is visible even when an in-memory
// datastore makes it look free. It composes with NewDelayReader: wrap the
// counter outermost to count the same calls the delay shim slows down.
//
// Queries reports every call; DistinctQueries reports the number of distinct
// (filter, arguments) tuples among them. A large gap between the two means the
// execution is re-reading identical rows within a single request.
//
// CountingReader is safe for concurrent use.
type CountingReader struct {
	inner QueryDatastoreReader

	mu       sync.Mutex
	queries  int             // GUARDED_BY(mu)
	distinct map[string]bool // GUARDED_BY(mu)
}

var _ QueryDatastoreReader = &CountingReader{}

// NewCountingReader wraps inner with a shim that counts every datastore call.
func NewCountingReader(inner QueryDatastoreReader) *CountingReader {
	return &CountingReader{inner: inner, distinct: make(map[string]bool)}
}

// record notes a single datastore round-trip against the given query key.
func (r *CountingReader) record(parts ...string) {
	key := strings.Join(parts, "|")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries++
	r.distinct[key] = true
}

// Queries returns the total number of datastore calls made so far.
func (r *CountingReader) Queries() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.queries
}

// DistinctQueries returns the number of distinct datastore calls made so far.
// Queries() minus this value is the number of calls that re-read rows an
// earlier call in the same execution already read.
func (r *CountingReader) DistinctQueries() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.distinct)
}

// Reset zeroes the counters, so a benchmark can exclude warm-up iterations.
func (r *CountingReader) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = 0
	clear(r.distinct)
}

func (r *CountingReader) CheckRelationships(ctx context.Context, filter CheckFilter) (PathSeq, error) {
	r.record("check", filter.ResourceType, strings.Join(filter.ResourceIDs, ","), filter.ResourceRelation,
		filter.SubjectType, strings.Join(filter.SubjectIDs, ","), filter.SubjectRelation)
	return r.inner.CheckRelationships(ctx, filter)
}

func (r *CountingReader) QuerySubjects(
	ctx context.Context,
	resource Object,
	resourceRelation string,
	subjectType ObjectType,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	r.record("subjects", resource.ObjectType, resource.ObjectID, resourceRelation, subjectType.String())
	return r.inner.QuerySubjects(ctx, resource, resourceRelation, subjectType, withCaveats, withExpiration, page)
}

func (r *CountingReader) QueryResources(
	ctx context.Context,
	resourceType string,
	resourceRelation string,
	subject ObjectAndRelation,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	r.record("resources", resourceType, resourceRelation, subject.String())
	return r.inner.QueryResources(ctx, resourceType, resourceRelation, subject, withCaveats, withExpiration, page)
}

func (r *CountingReader) SubjectExistsAsRelationship(
	ctx context.Context,
	subject Object,
	nonEllipsisRelation string,
) (bool, error) {
	r.record("exists", subject.ObjectType, subject.ObjectID, nonEllipsisRelation)
	return r.inner.SubjectExistsAsRelationship(ctx, subject, nonEllipsisRelation)
}

// LookupCaveatDefinition is not counted: implementations are expected to cache
// caveat definitions, so it is not a per-query round-trip.
func (r *CountingReader) LookupCaveatDefinition(
	ctx context.Context,
	name string,
) (datastore.CaveatDefinition, error) {
	return r.inner.LookupCaveatDefinition(ctx, name)
}
