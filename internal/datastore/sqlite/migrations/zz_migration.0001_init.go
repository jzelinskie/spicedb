package migrations

import (
	"context"
	_ "embed"

	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

//go:embed init_schema.sql
var initSchema string

func init() {
	SQLiteMigrations.MustRegister("initial", "", noNonAtomicMigration,
		func(ctx context.Context, txn *sqlite.Conn) error {
			if err := sqlitex.ExecScript(txn, initSchema); err != nil {
				return err
			}

			// Create unique datastore ID
			if err := sqlitex.Execute(txn,
				"INSERT INTO metadata (key, value) VALUES (?, ?)",
				&sqlitex.ExecOptions{Args: []any{"datastore_id", uuid.NewString()}},
			); err != nil {
				return err
			}

			// Insert empty revision.
			return sqlitex.Execute(txn, `INSERT INTO alembic_version (version_num) VALUES ('');`, nil)
		},
	)
}
