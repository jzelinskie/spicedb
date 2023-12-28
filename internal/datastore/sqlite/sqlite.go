package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

var tracer = otel.Tracer("spicedb/internal/datastore/sqlite")

const livingTxID = 9223372036854775807

func (ds *sqliteDatastore) queryMetadataValue(conn *sqlite.Conn, key string) (string, error) {
	stmt := conn.Prep(`SELECT value FROM metadata WHERE key = $key`)
	stmt.SetText("$key", key)
	return sqlitex.ResultText(stmt)
}

func (ds *sqliteDatastore) queryEstRelCount(conn *sqlite.Conn) (uint64, error) {
	stmt := conn.Prep(`SELECT COUNT(id) FROM relationship WHERE deleted_txn = $living_txn`)
	stmt.SetInt64("$living_txn", livingTxID)
	count, err := sqlitex.ResultInt64(stmt)
	return uint64(count), err
}

func (*sqliteDatastore) queryHeadRevision(conn *sqlite.Conn) (datastore.Revision, error) {
	stmt := conn.Prep(`SELECT MAX(id) FROM txn LIMIT 1`)
	txID, err := sqlitex.ResultInt64(stmt)
	if err != nil {
		return datastore.NoRevision, err
	}
	return revFromTx(txID)
}

func (ds *sqliteDatastore) optimizedRevisionFunc(ctx context.Context) (rev datastore.Revision, validFor time.Duration, err error) {
	conn := ds.pool.Get(ctx)
	if conn == nil {
		err = errors.New("failed to get connection from pool")
		return
	}
	defer ds.pool.Put(conn)

	err = sqlitex.Execute(conn, `SELECT COALESCE (
		(SELECT MIN(id)
		 FROM txn
		 WHERE time >= CAST(unixepoch('subsec') * 1000000000 / $quantization_nanos AS INTEGER) * $quantization_nanos / 1000000000),
		(SELECT MAX(id)
		 FROM txn)
	) AS revision,
	($quantization_nanos - (unixepoch('subsec') * 1000000000)) % $quantization_nanos`,
		&sqlitex.ExecOptions{
			Named: map[string]any{
				"$quantization_nanos": strconv.FormatInt(ds.opts.revisionQuantization.Nanoseconds(), 10),
			},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				rev, err = revFromTx(stmt.ColumnInt64(0))
				if err != nil {
					return err
				}
				validFor = time.Duration(stmt.ColumnInt64(1))
				return nil
			},
		},
	)
	return rev, validFor, err
}

func (ds *sqliteDatastore) queryCheckRevision(conn *sqlite.Conn, rev datastore.Revision) (fresh, unknown bool, err error) {
	err = sqlitex.Execute(conn,
		`SELECT $txid >= COALESCE(
			(SELECT MIN(id)
			 FROM txn
			 WHERE time >= (CAST(unixepoch('subsec') * 1000 AS INTEGER) - $gc_window_ms)),
			(SELECT MAX(id) FROM txn LIMIT 1)
		) as fresh,
		$txid > (SELECT MAX(id) FROM txn) AS unknown`,
		&sqlitex.ExecOptions{
			Named: map[string]any{
				"$txid":         revToTx(rev),
				"$gc_window_ms": ds.opts.gcWindow.Milliseconds(),
			},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				fresh = stmt.GetInt64("fresh") == 1
				unknown = stmt.GetInt64("unknown") == 1
				return nil
			},
		},
	)
	return fresh, unknown, err
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

	flags := sqlite.OpenReadWrite |
		sqlite.OpenCreate |
		sqlite.OpenWAL |
		sqlite.OpenURI

	p, err := sqlitex.Open(uri, flags, opts.poolSize)
	if err != nil {
		return nil, err
	}

	maxRevisionStaleness := time.Duration(float64(opts.revisionQuantization.Nanoseconds())*
		opts.maxRevisionStalenessPercent) * time.Nanosecond

	ds := &sqliteDatastore{
		CachedOptimizedRevisions: revisions.NewCachedOptimizedRevisions(maxRevisionStaleness),

		opts: opts,
		pool: p,
	}
	ds.SetOptimizedRevisionFunc(ds.optimizedRevisionFunc)
	return ds, nil
}

type sqliteDatastore struct {
	revision.DecimalDecoder
	*revisions.CachedOptimizedRevisions

	opts *sqliteOptions
	pool *sqlitex.Pool
}

func (ds *sqliteDatastore) CheckRevision(ctx context.Context, rev datastore.Revision) error {
	if rev == datastore.NoRevision {
		return datastore.NewInvalidRevisionErr(rev, datastore.CouldNotDetermineRevision)
	}

	conn := ds.pool.Get(ctx)
	if conn == nil {
		return errors.New("failed to get connection from pool")
	}
	defer ds.pool.Put(conn)

	if fresh, unknown, err := ds.queryCheckRevision(conn, rev); err != nil {
		return err
	} else if !fresh {
		return datastore.NewInvalidRevisionErr(rev, datastore.RevisionStale)
	} else if unknown {
		return datastore.NewInvalidRevisionErr(rev, datastore.CouldNotDetermineRevision)
	}

	return nil
}

func (ds *sqliteDatastore) Close() error { return ds.pool.Close() }

func (ds *sqliteDatastore) Features(_ context.Context) (*datastore.Features, error) {
	return &datastore.Features{
		Watch: datastore.Feature{Enabled: true},
	}, nil
}

func (ds *sqliteDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	conn := ds.pool.Get(ctx)
	if conn == nil {
		return nil, errors.New("failed to get connection from pool")
	}
	defer ds.pool.Put(conn)

	return ds.queryHeadRevision(conn)
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

func (*sqliteDatastore) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{IsReady: true}, nil
}

func (ds *sqliteDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	// This is nil-checked by the reader because it cannot be done here.
	conn := ds.pool.Get(context.Background())
	return &sqliteReader{
		conn:     conn,
		snapshot: &rev,
		executor: common.QueryExecutor{Executor: newSQLiteExecutor(conn)},
	}
}

func queryCreateTxn(conn *sqlite.Conn) (rev datastore.Revision, err error) {
	err = sqlitex.Execute(
		conn,
		"INSERT INTO txn (time) SELECT unixepoch('subsec') * 1000 RETURNING id",
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) (resultErr error) {
				rev, resultErr = revFromTx(stmt.GetInt64("id"))
				return resultErr
			},
		},
	)
	return rev, err
}

func (ds *sqliteDatastore) ReadWriteTx(ctx context.Context, txnFn datastore.TxUserFunc, opts ...options.RWTOptionsOption) (rev datastore.Revision, err error) {
	conn := ds.pool.Get(ctx)
	if conn == nil {
		return datastore.NoRevision, errors.New("failed to get connection from pool")
	}
	defer ds.pool.Put(conn)
	defer sqlitex.Save(conn)(&err)

	// TODO(jzelinskie): add retry logic
	// config := options.NewRWTOptionsWithOptions(opts...)

	fmt.Println("createTxn")
	rev, err = queryCreateTxn(conn)
	if err != nil {
		return datastore.NoRevision, err
	}
	fmt.Println("/createTxn")

	err = txnFn(ctx, &sqliteReaderWriter{
		sqliteReader: &sqliteReader{
			conn:     conn,
			snapshot: nil,
			executor: common.QueryExecutor{Executor: newSQLiteExecutor(conn)},
		},
		txID: revToTx(rev),
	})
	return rev, err
}

func (ds *sqliteDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	conn := ds.pool.Get(ctx)
	if conn == nil {
		return datastore.Stats{}, errors.New("failed to get connection from pool")
	}
	defer ds.pool.Put(conn)

	datastoreID, err := ds.queryMetadataValue(conn, "datastore_id")
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed querying unique datastore ID: %w", err)
	}

	estRelCount, err := ds.queryEstRelCount(conn)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed querying estimated relationship count: %w", err)
	}

	nsDefs, err := (&sqliteReader{
		conn:     conn,
		snapshot: nil,
		executor: common.QueryExecutor{Executor: newSQLiteExecutor(conn)},
	}).ListAllNamespaces(ctx)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed querying namespaces: %w", err)
	}

	return datastore.Stats{
		UniqueID:                   datastoreID,
		EstimatedRelationshipCount: estRelCount,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
	}, nil
}

func (*sqliteDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	panic("unimplemented")
}

var _ datastore.Datastore = (*sqliteDatastore)(nil)
