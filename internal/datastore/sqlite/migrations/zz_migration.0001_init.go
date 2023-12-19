package migrations

import (
	"context"
	_ "embed"
	"fmt"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/google/uuid"
)

//go:embed init_schema.sql
var initSchema string

func datastoreIDSQL() string {
	return fmt.Sprintf(`INSERT INTO metadata (key, value) VALUES ("datastore_id", "%s");`, uuid.NewString())
}

func init() {
	SQLiteMigrations.MustRegister("initial", "", noNonAtomicMigration,
		func(ctx context.Context, txn *sqlite.Conn) error {
			if err := sqlitex.ExecScript(txn, initSchema); err != nil {
				return err
			}

			// Create unique datastore ID
			if err := sqlitex.Exec(txn,
				"INSERT INTO metadata (key, value) VALUES (?, ?)",
				nil,
				"datastore_id",
				uuid.NewString(),
			); err != nil {
				return err
			}

			// Insert empty revision.
			return sqlitex.Exec(txn, `INSERT INTO alembic_version (version_num) VALUES ('');`, nil)
		},
	)
}
