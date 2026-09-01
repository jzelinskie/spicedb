package benchmarks

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/query"
)

// Round-trip metrics reported by the benchmarks in this package:
//
//	ds-queries/op        datastore queries issued by one operation
//	distinct-queries/op  how many of those were distinct (filter, arguments) tuples
//
// ns/op against memdb says nothing about the metric that bounds production
// workloads, where the connection pool is the scarce resource: the number of
// datastore queries. A change that replaces one batched query with a
// per-element loop is nearly free in these benchmarks and very expensive on a
// real datastore, so the counts are reported alongside the timings to make that
// visible. A gap between the two metrics is duplicate reads within a single
// request — work that coalescing or a request-scoped cache would remove.
//
// Counts are measured against the *advised* plan, which is what production
// runs. The advisor's arrow reversal can change them by an order of magnitude,
// so a count taken from an unadvised outline is not meaningful.
const (
	queriesMetric  = "ds-queries/op"
	distinctMetric = "distinct-queries/op"
)

// reportRoundTrips runs the operation once against a counting reader and
// reports how many datastore round-trips it took.
//
// The operation runs twice: once discarded, to absorb one-shot work such as
// schema loading, and once counted. Counts are already per-operation, so they
// are reported as-is rather than divided by the iteration count.
func reportRoundTrips(b *testing.B, inner query.QueryDatastoreReader, run func(query.QueryDatastoreReader)) {
	b.Helper()

	counter := query.NewCountingReader(inner)
	run(counter)
	counter.Reset()
	run(counter)

	b.ReportMetric(float64(counter.Queries()), queriesMetric)
	b.ReportMetric(float64(counter.DistinctQueries()), distinctMetric)
}

// countingDatastore wraps a datastore so the classic dispatcher's reads can be
// counted the same way the query planner's are. The planner reads through
// query.QueryDatastoreReader and the classic dispatcher reads through
// datastore.Reader, so comparing the two requires counting at the lower,
// common layer.
type countingDatastore struct {
	datastore.Datastore
	counter *datastoreCounter
}

func (d countingDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return countingDatastoreReader{d.Datastore.SnapshotReader(rev), d.counter}
}

// datastoreCounter tallies queries issued through a countingDatastore.
type datastoreCounter struct {
	mu       sync.Mutex
	queries  int             // GUARDED_BY(mu)
	distinct map[string]bool // GUARDED_BY(mu)
}

func newDatastoreCounter() *datastoreCounter {
	return &datastoreCounter{distinct: make(map[string]bool)}
}

func (c *datastoreCounter) record(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries++
	c.distinct[key] = true
}

func (c *datastoreCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = 0
	clear(c.distinct)
}

func (c *datastoreCounter) stats() (queries, distinct int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queries, len(c.distinct)
}

type countingDatastoreReader struct {
	datastore.Reader
	counter *datastoreCounter
}

func (r countingDatastoreReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	r.counter.record(fmt.Sprintf("forward|%s#%s|%v|%v",
		filter.OptionalResourceType, filter.OptionalResourceRelation,
		filter.OptionalResourceIds, filter.OptionalSubjectsSelectors))
	return r.Reader.QueryRelationships(ctx, filter, opts...)
}

func (r countingDatastoreReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	r.counter.record(fmt.Sprintf("reverse|%s|%v|%v",
		subjectsFilter.SubjectType, subjectsFilter.OptionalSubjectIds, subjectsFilter.RelationFilter))
	return r.Reader.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
}
