package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/authzed/spicedb/pkg/migrate"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("spicedb/internal/datastore/common")

// AlembicSQLiteDriver implements a schema migration facility for use in
// SpiceDB's SQLite datastore.
//
// It is stores revisions similar to the popular Python library, Alembic.
type AlembicSQLiteDriver struct {
	db *sqlite.Conn
}

// NewAlembicSQLiteDriver creates a new driver with an active connections to
// the database specified.
func NewAlembicSQLiteDriver(ctx context.Context, uri string) (*AlembicSQLiteDriver, error) {
	ctx, span := tracer.Start(ctx, "NewAlembicSQLiteDriver")
	defer span.End()

	// 0 uses the following flags for defaults:
	//   SQLITE_OPEN_READWRITE
	//   SQLITE_OPEN_CREATE
	//   SQLITE_OPEN_WAL
	//   SQLITE_OPEN_URI
	//   SQLITE_OPEN_NOMUTEX
	db, err := sqlite.OpenConn(uri, 0)
	if err != nil {
		return nil, err
	}

	// TODO(jzelinskie): implement equivalents for logging/tracing
	// pgxcommon.ConfigurePGXLogger(connConfig)
	// pgxcommon.ConfigureOTELTracer(connConfig)

	return &AlembicSQLiteDriver{db}, nil
}

// Conn returns the underlying pgx.Conn instance for this driver
func (d *AlembicSQLiteDriver) Conn() *sqlite.Conn { return d.db }

func (d *AlembicSQLiteDriver) RunTx(ctx context.Context, f migrate.TxMigrationFunc[*sqlite.Conn]) (err error) {
	defer sqlitex.Save(d.db)(&err)
	err = f(ctx, d.db)
	return
}

// Version returns the version of the schema to which the connected database
// has been migrated.
func (d *AlembicSQLiteDriver) Version(ctx context.Context) (string, error) {
	stmt, err := d.db.Prepare("SELECT version_num FROM alembic_version")
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return "", nil // This is the first run
		}
		return "", err
	}
	return sqlitex.ResultText(stmt)
}

// Close disposes the driver.
func (d *AlembicSQLiteDriver) Close(_ context.Context) error { return d.db.Close() }

func (d *AlembicSQLiteDriver) WriteVersion(ctx context.Context, txn *sqlite.Conn, version, replaced string) error {
	if err := sqlitex.Exec(
		d.db,
		"UPDATE alembic_version SET version_num=? WHERE version_num=?",
		nil,
		version,
		replaced,
	); err != nil {
		return fmt.Errorf("unable to update version row: %w", err)
	}
	return nil
}

var _ migrate.Driver[*sqlite.Conn, *sqlite.Conn] = &AlembicSQLiteDriver{}

var (
	noNonAtomicMigration migrate.MigrationFunc[*sqlite.Conn]
	noAtomicMigration    migrate.TxMigrationFunc[*sqlite.Conn]
)

// SQLiteMigrations implements a migration manager for the CRDBDriver.
var SQLiteMigrations = migrate.NewManager[*AlembicSQLiteDriver, *sqlite.Conn, *sqlite.Conn]()
