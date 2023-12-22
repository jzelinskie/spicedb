package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"go.opentelemetry.io/otel"

	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

var tracer = otel.Tracer("spicedb/internal/datastore/sqlite")

const livingTxID = 9223372036854775807

func (ds *ds) queryMetadataValue(conn *sqlite.Conn, key string) *sqlite.Stmt {
	stmt := conn.Prep(`
		SELECT value
		FROM metadata
		WHERE key = $key;
	`)
	stmt.SetText("$key", key)
	return stmt
}

func (ds *ds) queryEstRelCount(conn *sqlite.Conn) *sqlite.Stmt {
	stmt := conn.Prep(`
		SELECT COUNT(id)
		FROM relationship
		WHERE deleted_txn = $living_txn;
	`)
	stmt.SetInt64("$living_txn", livingTxID)
	return stmt
}

func (*ds) queryHeadRevision(conn *sqlite.Conn) *sqlite.Stmt {
	return conn.Prep(`
		SELECT MAX(id)
		FROM txn
		LIMIT 1;
	`)
}

func (ds *ds) optimizedRevisionFunc(ctx context.Context) (rev datastore.Revision, validFor time.Duration, err error) {
	conn := ds.pool.Get(ctx)
	if conn == nil {
		err = errors.New("failed to get connection from pool")
		return
	}
	defer ds.pool.Put(conn)

	stmt := conn.Prep(`
	SELECT COALESCE ((
		SELECT MIN(id)
		FROM txn
		WHERE
			time >= ROUND(CAST(unixepoch('subsec') * 1000000000 / $quantization_nanos) * $quantization_nanos / 1000000000
	), (
		SELECT MAX(id)
		FROM id
	)) AS revision,
	($quantization_nanos - strftime('%s', 'subsec') * 1000000000) % $quantization_nanos);
	`)
	stmt.SetText("$quantization_nanos", strconv.FormatInt(ds.opts.revisionQuantization.Nanoseconds(), 10))

	if hasRow, stepErr := stmt.Step(); err != nil {
		err = stepErr
		return
	} else if !hasRow {
		panic("found no rows when executing optimizedRevisionFunc")
	}

	rev, err = revFromTx(stmt.ColumnInt64(0))
	validFor = time.Duration(stmt.ColumnInt64(1))
	return rev, validFor, err
}

func (ds *ds) queryCheckRevision(ctx context.Context, txnID int64) (fresh, unknown bool, err error) {
	conn := ds.pool.Get(ctx)
	if conn == nil {
		err = errors.New("failed to get connection from pool")
		return
	}
	defer ds.pool.Put(conn)
	stmt := conn.Prep(`
		SELECT $txid >= COALESCE(
			(SELECT MIN(id)
			 FROM txn
			 WHERE time >= (CAST(unixepoch('subsec') * 1000 AS INTEGER) - $gc_window_ms)),
			(SELECT MAX(id) FROM txn LIMIT 1)
		) as fresh,
		$txid > (SELECT MAX(id) FROM txn) AS unknown;
	`)
	stmt.SetInt64("$txid", txnID)
	stmt.SetInt64("$gc_window_ms", ds.opts.gcWindow.Milliseconds())

	freshInt, err := sqlitex.ResultInt(stmt)
	if err != nil {
		return fresh, unknown, err
	}

	unknownInt, err := sqlitex.ResultInt(stmt)
	if err != nil {
		return fresh, unknown, err
	}

	return freshInt == 1, unknownInt == 1, nil
}

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

const (
	Engine = "sqlite"
)

type sqliteOptions struct {
	poolSize                    int
	gcWindow                    time.Duration
	revisionQuantization        time.Duration
	maxRevisionStalenessPercent float64
}

func defaultOpts() *sqliteOptions {
	return &sqliteOptions{
		poolSize:                    10,
		gcWindow:                    24 * time.Hour,
		revisionQuantization:        5 * time.Second,
		maxRevisionStalenessPercent: 0.1,
	}
}

type Option func(*sqliteOptions)

func PoolSize(size int) Option {
	return func(opts *sqliteOptions) { opts.poolSize = size }
}

func GCWindow(d time.Duration) Option {
	return func(opts *sqliteOptions) { opts.gcWindow = d }
}

func MaxRevisionStalenessPercent(p float64) Option {
	return func(opts *sqliteOptions) { opts.maxRevisionStalenessPercent = p }
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

	flags := sqlite.SQLITE_OPEN_READWRITE |
		sqlite.SQLITE_OPEN_CREATE |
		sqlite.SQLITE_OPEN_WAL |
		sqlite.SQLITE_OPEN_URI |
		sqlite.SQLITE_OPEN_NOMUTEX

	p, err := sqlitex.Open(uri, flags, opts.poolSize)
	if err != nil {
		return nil, err
	}

	maxRevisionStaleness := time.Duration(float64(opts.revisionQuantization.Nanoseconds())*
		opts.maxRevisionStalenessPercent) * time.Nanosecond

	ds := &ds{
		CachedOptimizedRevisions: revisions.NewCachedOptimizedRevisions(maxRevisionStaleness),

		opts: opts,
		pool: p,
	}
	ds.SetOptimizedRevisionFunc(ds.optimizedRevisionFunc)
	return ds, nil
}

type ds struct {
	revision.DecimalDecoder
	*revisions.CachedOptimizedRevisions

	opts *sqliteOptions
	pool *sqlitex.Pool
}

func (ds *ds) CheckRevision(ctx context.Context, rev datastore.Revision) error {
	if rev == datastore.NoRevision {
		return datastore.NewInvalidRevisionErr(rev, datastore.CouldNotDetermineRevision)
	}

	if fresh, unknown, err := ds.queryCheckRevision(ctx, revToTx(rev)); err != nil {
		return err
	} else if !fresh {
		return datastore.NewInvalidRevisionErr(rev, datastore.RevisionStale)
	} else if unknown {
		return datastore.NewInvalidRevisionErr(rev, datastore.CouldNotDetermineRevision)
	}

	return nil
}

func (ds *ds) Close() error { return ds.pool.Close() }

func (ds *ds) Features(_ context.Context) (*datastore.Features, error) {
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

	rev, err := sqlitex.ResultInt64(ds.queryHeadRevision(conn))
	if err != nil {
		return nil, err
	}

	return revFromTx(rev)
}

func revFromTx(txid int64) (datastore.Revision, error) {
	if txid < 0 {
		return nil, errors.New("negative transaction ID")
	}

	return revision.NewFromUint64(uint64(txid)), nil
}

func revToTx(rev datastore.Revision) int64 {
	return rev.(revision.Decimal).IntPart()
}

func (*ds) ReadWriteTx(context.Context, datastore.TxUserFunc, ...options.RWTOptionsOption) (datastore.Revision, error) {
	panic("unimplemented")
}

func (*ds) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{IsReady: true}, nil
}

func (*ds) SnapshotReader(datastore.Revision) datastore.Reader { panic("unimplemented") }

func (ds *ds) Statistics(ctx context.Context) (datastore.Stats, error) {
	conn := ds.pool.Get(ctx)
	if conn == nil {
		return datastore.Stats{}, errors.New("failed to get connection from pool")
	}
	defer ds.pool.Put(conn)

	datastoreID, err := sqlitex.ResultText(ds.queryMetadataValue(conn, "datastore_id"))
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed querying unique datastore ID: %w", err)
	}

	estRelCount, err := sqlitex.ResultInt64(ds.queryEstRelCount(conn))
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed querying estimated relationship count: %w", err)
	}

	nsDefs := []datastore.RevisionedNamespace{} // TODO(jzelinskie): implement loading namespaces

	return datastore.Stats{
		UniqueID:                   datastoreID,
		EstimatedRelationshipCount: uint64(estRelCount),
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
	}, nil
}

func (*ds) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	panic("unimplemented")
}

var _ datastore.Datastore = (*ds)(nil)
