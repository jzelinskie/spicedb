package sqlite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"crawshaw.io/sqlite/sqlitex"
	"go.opentelemetry.io/otel"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

var tracer = otel.Tracer("spicedb/internal/datastore/sqlite")

var queryTemplates = map[string]func(*sqliteOptions) string{
	"HeadRevision": func(*sqliteOptions) string { return "SELECT MAX(id) FROM txn LIMIT 1;" },
	"CheckRevision": func(opts *sqliteOptions) string {
		return fmt.Sprintf(`
			SELECT $txid >= COALESCE((
				SELECT MIN(id)
				FROM   txn
				WHERE  time >= TIMESTAMPADD(SECOND, %.6[1]f, UTC_TIMESTAMP(6))
			), ( 
		    	SELECT MAX(id)
		    	FROM txn
		    	LIMIT 1
			)) as fresh, $txid > (
				SELECT MAX(id)
				FROM   txn
			) as unknown;`, -1*opts.gcWindow.Seconds())
	},
	"OptimizedRevision": func(opts *sqliteOptions) string {
		return fmt.Sprintf(`
			SELECT COALESCE((
				SELECT MIN(id)
				FROM   txn
				WHERE  time >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) * 1000000000 / %[1]d) * %[1]d / 1000000000)
			), (
				SELECT MAX(id)
				FROM   id
			)) as revision,
			%[1]d - CAST(UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) * 1000000000 AS UNSIGNED INTEGER) %% %[1]d as validForNanos;`, opts.revisionQuantization.Nanoseconds())
	},
}

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

const (
	Engine = "sqlite"
)

type sqliteOptions struct {
	poolSize             int
	gcWindow             time.Duration
	revisionQuantization time.Duration
}

func defaultOpts() *sqliteOptions {
	return &sqliteOptions{
		poolSize:             10,
		gcWindow:             24 * time.Hour,
		revisionQuantization: 5 * time.Second,
	}
}

type Option func(*sqliteOptions)

func PoolSize(size int) Option {
	return func(opts *sqliteOptions) { opts.poolSize = size }
}

func GCWindow(d time.Duration) Option {
	return func(opts *sqliteOptions) { opts.gcWindow = d }
}

func RevisionQuantization(d time.Duration) Option {
	return func(opts *sqliteOptions) {
		if d.Nanoseconds() < 1 {
			d = time.Duration(1)
		}
		opts.revisionQuantization = d
	}
}

func NewDatastore(uri string, options ...Option) (datastore.Datastore, error) {
	opts := defaultOpts()
	for _, option := range options {
		option(opts)
	}

	queries := make(map[string]string, len(queryTemplates))
	for query, tmplfn := range queryTemplates {
		queries[query] = tmplfn(opts)
	}

	p, err := sqlitex.Open(uri, 0, opts.poolSize)
	if err != nil {
		return nil, err
	}
	ds := &ds{pool: p}

	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

type ds struct {
	revision.DecimalDecoder

	pool    *sqlitex.Pool
	queries map[string]string
}

func (ds *ds) CheckRevision(ctx context.Context, rev datastore.Revision) error {
	if rev == datastore.NoRevision {
		return datastore.NewInvalidRevisionErr(rev, datastore.CouldNotDetermineRevision)
	}

	{
		conn := ds.pool.Get(ctx)
		if conn == nil {
			return errors.New("failed to get connection from pool")
		}
		defer ds.pool.Put(conn)

		_, span := tracer.Start(ctx, "CheckRevisionQuery")
		defer span.End()

		stmt := conn.Prep(ds.queries["CheckRevision"])
		stmt.SetInt64("$txid", rev.(revision.Decimal).IntPart())

		hasFresh, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasFresh {
			return errors.New("failed to determine fresh")
		}
		if stmt.GetInt64("fresh") != 1 {
			return datastore.NewInvalidRevisionErr(rev, datastore.RevisionStale)
		}

		hasUnknown, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasUnknown {
			return errors.New("failed to determine unknown")
		}
		if stmt.GetInt64("unknown") == 1 {
			return datastore.NewInvalidRevisionErr(rev, datastore.CouldNotDetermineRevision)
		}

		return nil
	}
}

func (ds *ds) Close() error { return ds.pool.Close() }

func (*ds) Features(_ context.Context) (*datastore.Features, error) {
	return &datastore.Features{
		Watch: datastore.Feature{Enabled: true},
	}, nil
}

func (ds *ds) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	conn := ds.pool.Get(ctx)
	if conn == nil {
		return nil, errors.New("failed to get connection from pool")
	}
	defer ds.pool.Put(conn)

	stmt := conn.Prep(ds.queries["HeadRevision"])
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, errors.New("no transactions found")
	}

	return revFromTx(stmt.GetInt64("id"))
}

func revFromTx(txid int64) (datastore.Revision, error) {
	if txid < 0 {
		return nil, errors.New("negative transaction ID")
	}

	return revision.NewFromUint64(uint64(txid)), nil
}

func (ds *ds) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	conn := ds.pool.Get(ctx)
	if conn == nil {
		return nil, errors.New("failed to get connection from pool")
	}
	defer ds.pool.Put(conn)

	stmt := conn.Prep(ds.queries["OptimizedRevision"])
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, errors.New("no transactions found")
	}

	return revFromTx(stmt.GetInt64("id"))
}

func (*ds) ReadWriteTx(context.Context, datastore.TxUserFunc, ...options.RWTOptionsOption) (datastore.Revision, error) {
	panic("unimplemented")
}

func (*ds) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{IsReady: true}, nil
}

func (*ds) SnapshotReader(datastore.Revision) datastore.Reader { panic("unimplemented") }

func (*ds) Statistics(ctx context.Context) (datastore.Stats, error) { panic("unimplemented") }

func (*ds) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	panic("unimplemented")
}

var _ datastore.Datastore = (*ds)(nil)
