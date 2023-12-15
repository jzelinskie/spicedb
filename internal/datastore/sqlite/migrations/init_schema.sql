PRAGMA strict_types = ON;

CREATE TABLE IF NOT EXISTS alembic_version ( version_num TEXT NOT NULL );

CREATE TABLE IF NOT EXISTS metadata (
  key   TEXT NOT NULL PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS txn (
  id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  time TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_txn_by_time ON txn ( time );

CREATE TABLE IF NOT EXISTS namespace (
  name              TEXT NOT NULL,
  serialized_config BLOB NOT NULL,
  created_txn       INTEGER NOT NULL,
  deleted_txn       INTEGER NOT NULL DEFAULT 9223372036854775807,
  CONSTRAINT pk_namespace PRIMARY KEY ( name, created_txn ),
  CONSTRAINT uq_living_namespace UNIQUE ( name, deleted_txn )
);

CREATE TABLE IF NOT EXISTS relationship (
  id               INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  object_type      TEXT NOT NULL,
  object_id        TEXT NOT NULL,
  object_relation  TEXT NOT NULL,
  subject_type     TEXT NOT NULL,
  subject_id       TEXT NOT NULL,
  subject_relation TEXT NOT NULL,
  created_txn      INTEGER NOT NULL,
  deleted_txn      INTEGER NOT NULL DEFAULT 9223372036854775807,
  CONSTRAINT uq_relationship UNIQUE (
    object_type,  object_id,  object_relation,
    subject_type, subject_id, subject_relation,
    created_txn, deleted_txn
  ),
  CONSTRAINT uq_living_relationship UNIQUE (
    object_type,  object_id,  object_relation,
    subject_type, subject_id, subject_relation,
    deleted_txn
  )
);
CREATE INDEX IF NOT EXISTS idx_relationship_by_subject ON relationship (
  subject_id, subject_type, subject_relation,
  object_type, object_relation
);
CREATE INDEX IF NOT EXISTS idx_relationship_by_subject_relation ON relationship (
  subject_type, subject_relation,
  object_type, object_relation
);
CREATE INDEX IF NOT EXISTS idx_relationship_by_deletion ON relationship ( deleted_txn );
CREATE INDEX IF NOT EXISTS idx_relationship_watch ON relationship (
  created_txn, deleted_txn DESC
);
CREATE TABLE IF NOT EXISTS caveat (
  name TEXT NOT NULL,
  expression BLOB NOT NULL,
  created_txn INTEGER NOT NULL,
  deleted_txn INTEGER NOT NULL DEFAULT 9223372036854775807,
  CONSTRAINT pk_caveat PRIMARY KEY ( name, deleted_txn ),
  CONSTRAINT uq_caveat UNIQUE ( name, created_txn, deleted_txn )
);
