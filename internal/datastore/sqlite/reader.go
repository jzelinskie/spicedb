package sqlite

import (
	"context"
	"encoding/json"
	"errors"

	sq "github.com/Masterminds/squirrel"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type sqliteReader struct {
	conn     *sqlite.Conn
	snapshot *datastore.Revision
	executor common.QueryExecutor
}

func newSQLiteExecutor(conn *sqlite.Conn) common.ExecuteQueryFunc {
	return func(ctx context.Context, sqlQuery string, args []any) ([]*corev1.RelationTuple, error) {
		var rels []*corev1.RelationTuple
		err := sqlitex.Execute(conn, sqlQuery, &sqlitex.ExecOptions{
			Args: args,
			ResultFunc: func(stmt *sqlite.Stmt) error {
				var caveatContext map[string]any
				if cc := stmt.GetText("caveat_context"); cc != "" {
					if err := json.Unmarshal([]byte(cc), &caveatContext); err != nil {
						return err
					}
				}

				caveatName := stmt.GetText("caveat_name")
				caveat, err := common.ContextualizedCaveatFrom(caveatName, caveatContext)
				if err != nil {
					return err
				}

				rels = append(rels, &corev1.RelationTuple{
					ResourceAndRelation: &corev1.ObjectAndRelation{
						Namespace: stmt.GetText("object_type"),
						ObjectId:  stmt.GetText("object_id"),
						Relation:  stmt.GetText("object_relation"),
					},
					Subject: &corev1.ObjectAndRelation{
						Namespace: stmt.GetText("subject_type"),
						ObjectId:  stmt.GetText("subject_id"),
						Relation:  stmt.GetText("subject_relation"),
					},
					Caveat: caveat,
				})
				return nil
			},
		})

		return rels, err
	}
}

func queryNamespaces(conn *sqlite.Conn, rev *datastore.Revision, names ...string) (namespaces []datastore.RevisionedNamespace, err error) {
	if conn == nil {
		return nil, errors.New("failed to get connection from pool")
	}

	sb := sq.Select("serialized_config", "created_txn").From("namespace")

	// Add the clauses to query at a snapshot or fully consistent.
	if rev != nil {
		txid := revToTx(*rev)
		sb = sb.Where(sq.And{
			sq.LtOrEq{"created_txn": txid},
			sq.Or{
				sq.Eq{"deleted_txn": livingTxID},
				sq.Gt{"deleted_txn": txid},
			},
		})
	} else {
		sb = sb.Where(sq.Eq{"deleted_txn": livingTxID})
	}

	// Add the clauses for filtering by name.
	if len(names) > 0 {
		namesClause := sq.Or{}
		for _, name := range names {
			namesClause = append(namesClause, sq.Eq{"name": name})
		}
		sb = sb.Where(namesClause)
	}

	query, values, err := sb.ToSql()
	if err != nil {
		return nil, err
	}

	var buf []byte
	err = sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
		Args:       values,
		ResultFunc: scanNamespace(namespaces, buf),
	})
	return namespaces, err
}

func scanNamespace(namespaces []datastore.RevisionedNamespace, scratchBuf []byte) execResultFunc {
	return func(stmt *sqlite.Stmt) error {
		namespace := &corev1.NamespaceDefinition{}
		stmt.GetBytes("serialized_config", scratchBuf)
		if err := namespace.UnmarshalVT(scratchBuf); err != nil {
			return err
		}
		scratchBuf = scratchBuf[:0] // Reset the buffer

		rev, err := revFromTx(stmt.GetInt64("created_txn"))
		if err != nil {
			return err
		}

		namespaces = append(namespaces, datastore.RevisionedNamespace{
			Definition:          namespace,
			LastWrittenRevision: rev,
		})
		return nil
	}
}

func queryCaveats(conn *sqlite.Conn, rev *datastore.Revision, names ...string) (caveats []datastore.RevisionedCaveat, err error) {
	if conn == nil {
		return nil, errors.New("failed to get connection from pool")
	}

	sb := sq.Select("expression", "created_txn").From("caveat").OrderBy("name")

	if rev != nil {
		txID := revToTx(*rev)
		sb = sb.Where(sq.And{
			sq.LtOrEq{"created_txn": txID},
			sq.Or{
				sq.Eq{"deleted_txn": livingTxID},
				sq.Gt{"deleted_txn": txID},
			},
		})
	} else {
		sb = sb.Where(sq.Eq{"deleted_txn": livingTxID})
	}

	query, values, err := sb.ToSql()
	if err != nil {
		return nil, err
	}

	var buf []byte
	err = sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
		Args:       values,
		ResultFunc: scanCaveat(caveats, buf),
	})
	return caveats, err
}

// execResultFunc is the callback used by sqlitex.Exec after each successful
// row returned from a *sqlite.Stmt.
type execResultFunc func(*sqlite.Stmt) error

func scanCaveat(caveats []datastore.RevisionedCaveat, scratchBuf []byte) execResultFunc {
	return func(stmt *sqlite.Stmt) error {
		caveat := &corev1.CaveatDefinition{}
		stmt.GetBytes("expression", scratchBuf)
		if err := caveat.UnmarshalVT(scratchBuf); err != nil {
			return err
		}
		scratchBuf = scratchBuf[:0] // Reset the buffer

		rev, err := revFromTx(stmt.GetInt64("created_txn"))
		if err != nil {
			return err
		}

		caveats = append(caveats, datastore.RevisionedCaveat{
			Definition:          caveat,
			LastWrittenRevision: rev,
		})

		return nil
	}
}

// ListAllCaveats implements datastore.Reader.
func (r *sqliteReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return queryCaveats(r.conn, r.snapshot)
}

// LookupCaveatsWithNames implements datastore.Reader.
func (r *sqliteReader) LookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	if len(names) == 0 {
		return nil, nil
	}
	return queryCaveats(r.conn, r.snapshot, names...)
}

// ListAllNamespaces implements datastore.Reader.
func (r *sqliteReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedDefinition[*corev1.NamespaceDefinition], error) {
	return queryNamespaces(r.conn, r.snapshot)
}

// LookupNamespacesWithNames implements datastore.Reader.
func (r *sqliteReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if len(nsNames) == 0 {
		return nil, nil
	}
	return queryNamespaces(r.conn, r.snapshot, nsNames...)
}

var schema = common.NewSchemaInformation(
	"object_type",
	"object_id",
	"object_relation",
	"subject_type",
	"subject_id",
	"subject_relation",
	"caveat",
	common.ExpandedLogicComparison,
)

var queryTuples = sq.Select(
	"object_type", "object_id", "object_relation",
	"subject_type", "subject_id", "subject_relation",
	"caveat_name", "caveat_context",
).From("relationship")

// QueryRelationships implements datastore.Reader.
func (r *sqliteReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	if r.conn == nil {
		return nil, errors.New("failed to get connection from pool")
	}

	b, err := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterWithRelationshipsFilter(filter)
	if err != nil {
		return nil, err
	}
	return r.executor.ExecuteQuery(ctx, b, opts...)
}

// ReverseQueryRelationships implements datastore.Reader.
func (r *sqliteReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	if r.conn == nil {
		return nil, errors.New("failed to get connection from pool")
	}

	b, err := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterWithSubjectsSelectors(subjectsFilter.AsSelector())
	if err != nil {
		return nil, err
	}

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)
	if queryOpts.ResRelation != nil {
		b = b.FilterToResourceType(queryOpts.ResRelation.Namespace).
			FilterToRelation(queryOpts.ResRelation.Relation)
	}

	return r.executor.ExecuteQuery(ctx, b,
		options.WithLimit(queryOpts.LimitForReverse),
		options.WithAfter(queryOpts.AfterForReverse),
		options.WithSort(queryOpts.SortForReverse),
	)
}

// ReadCaveatByName implements datastore.Reader.
func (r *sqliteReader) ReadCaveatByName(ctx context.Context, name string) (caveat *corev1.CaveatDefinition, lastWritten datastore.Revision, err error) {
	caveats, err := queryCaveats(r.conn, r.snapshot, name)
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	switch len(caveats) {
	case 0:
		return nil, datastore.NoRevision, datastore.NewCaveatNameNotFoundErr(name)
	case 1:
		return caveats[0].Definition, caveats[0].LastWrittenRevision, nil
	default:
		panic("found multiple caveats with name: " + name)
	}
}

// ReadNamespaceByName implements datastore.Reader.
func (r *sqliteReader) ReadNamespaceByName(ctx context.Context, name string) (ns *corev1.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	namespaces, err := queryNamespaces(r.conn, r.snapshot, name)
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	switch len(namespaces) {
	case 0:
		return nil, datastore.NoRevision, datastore.NewNamespaceNotFoundErr(name)
	case 1:
		return namespaces[0].Definition, namespaces[0].LastWrittenRevision, nil
	default:
		panic("found multiple namespaces with name: " + name)
	}
}

var _ datastore.Reader = (*sqliteReader)(nil)
