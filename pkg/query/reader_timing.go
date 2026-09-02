package query

import (
	"context"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
)

// delayReader is a QueryDatastoreReader shim that sleeps for a fixed duration
// before each call to simulate network latency. It delegates all logic to an
// inner reader.
type delayReader struct {
	delay time.Duration
	inner QueryDatastoreReader
}

// NewDelayReader wraps inner with a shim that sleeps for delay before every
// call. Use this in benchmarks to model realistic network round-trip costs.
func NewDelayReader(delay time.Duration, inner QueryDatastoreReader) QueryDatastoreReader {
	return &delayReader{delay: delay, inner: inner}
}

func (r *delayReader) sleep(ctx context.Context) error {
	select {
	case <-time.After(r.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *delayReader) CheckRelationships(ctx context.Context, filter CheckFilter) (PathSeq, error) {
	if err := r.sleep(ctx); err != nil {
		return nil, err
	}
	return r.inner.CheckRelationships(ctx, filter)
}

func (r *delayReader) QuerySubjects(ctx context.Context, filter SubjectsFilter) (PathSeq, error) {
	if err := r.sleep(ctx); err != nil {
		return nil, err
	}
	return r.inner.QuerySubjects(ctx, filter)
}

func (r *delayReader) QueryResources(ctx context.Context, filter ResourcesFilter) (PathSeq, error) {
	if err := r.sleep(ctx); err != nil {
		return nil, err
	}
	return r.inner.QueryResources(ctx, filter)
}

func (r *delayReader) LookupCaveatDefinition(
	ctx context.Context,
	name string,
) (datastore.CaveatDefinition, error) {
	if err := r.sleep(ctx); err != nil {
		return nil, err
	}
	return r.inner.LookupCaveatDefinition(ctx, name)
}
