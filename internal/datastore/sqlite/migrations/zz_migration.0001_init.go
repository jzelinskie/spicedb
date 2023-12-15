package migrations

import (
	"context"
	_ "embed"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
)

//go:embed init_schema.sql
var initSchema string

func init() {
	if err := SQLiteMigrations.Register("1", "", noNonAtomicMigration, func(ctx context.Context, txn *sqlite.Conn) error {
		return sqlitex.ExecScript(txn, initSchema)
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
