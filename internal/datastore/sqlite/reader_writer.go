package sqlite

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type sqliteReaderWriter struct {
	*sqliteReader

	txID int64
}

func (rw *sqliteReaderWriter) conn() *sqlite.Conn {
	return rw.sqliteReader.conn
}

// BulkLoad implements datastore.ReadWriteTransaction.
func (rw *sqliteReaderWriter) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	return 0, nil
}

// WriteRelationships implements datastore.ReadWriteTransaction.
func (rw *sqliteReaderWriter) WriteRelationships(ctx context.Context, updates []*corev1.RelationTupleUpdate) (err error) {
	// Remove any duplicates or conflicting updates.
	creates, touches, deletes, err := normalizeUpdates(updates)
	if err != nil {
		return err
	}

	// Iterate over the to-be-deleted relationships to build the necessary SQL
	// clauses.
	deletedRelClause := sq.Or{}
	for _, update := range deletes {
		deletedRelClause = append(deletedRelClause, sq.Eq{
			"object_type":      update.Tuple.ResourceAndRelation.Namespace,
			"object_id":        update.Tuple.ResourceAndRelation.ObjectId,
			"object_relation":  update.Tuple.ResourceAndRelation.Relation,
			"subject_type":     update.Tuple.Subject.Namespace,
			"subject_id":       update.Tuple.Subject.ObjectId,
			"subject_relation": update.Tuple.Subject.Relation,
		})
	}

	// Mark the relationships as deleted.
	if err := rw.exec(
		sq.Update("relationship").
			Set("deleted_txn", rw.txID).
			FromSelect(
				sq.Select("relationship").
					Where(deletedRelClause).
					Where(sq.GtOrEq{"deleted_txn": rw.txID}),
				"updates",
			).
			Where(sq.Eq{"id": "updates.id"}),
		nil,
	); err != nil {
		return err
	}

	// Iterate over the to-be-created relationships to build the necessary SQL
	// clauses.
	insertValues := make([]any, 0, (len(creates)+len(touches))*9)
	for _, update := range append(creates, touches...) {
		insertValues = append(insertValues,
			rw.txID,
			update.Tuple.ResourceAndRelation.Namespace,
			update.Tuple.ResourceAndRelation.ObjectId,
			update.Tuple.ResourceAndRelation.Relation,
			update.Tuple.Subject.Namespace,
			update.Tuple.Subject.ObjectId,
			update.Tuple.Subject.Relation,
		)
		if update.Tuple.Caveat != nil {
			contextJSON, err := update.Tuple.Caveat.Context.MarshalJSON()
			if err != nil {
				return err
			}
			insertValues = append(insertValues, update.Tuple.Caveat.CaveatName, contextJSON)
		} else {
			insertValues = append(insertValues, nil, nil)
		}
	}

	// Insert the new relationships.
	if len(insertValues) > 0 {
		return rw.exec(
			sq.Insert("relationship").Columns(
				"created_txn",
				"object_type",
				"object_id",
				"object_relation",
				"subject_type",
				"subject_id",
				"subject_relation",
				"caveat_name",
				"caveat_context",
			).Values(insertValues...),
			nil,
		)
	}

	return nil
}

// normalizeUpdates removes any duplicate or conflicting mutations.
//
// TODO(jzelinskie): This should be reusable for other datastores.
func normalizeUpdates(mutations []*corev1.RelationTupleUpdate) (creates, touches, deletes []*corev1.RelationTupleUpdate, err error) {
	createRels := make(map[string]*corev1.RelationTupleUpdate, len(mutations))
	touchRels := make(map[string]*corev1.RelationTupleUpdate, len(mutations))
	deleteRels := make(map[string]*corev1.RelationTupleUpdate, len(mutations))

	for _, mutation := range mutations {
		switch mutation.Operation {
		case corev1.RelationTupleUpdate_CREATE:
			createRels[tuple.MustString(mutation.Tuple)] = mutation
		case corev1.RelationTupleUpdate_TOUCH:
			touchRels[tuple.MustString(mutation.Tuple)] = mutation
		case corev1.RelationTupleUpdate_DELETE:
			deleteRels[tuple.MustString(mutation.Tuple)] = mutation
		default:
			return nil, nil, nil, spiceerrors.MustBugf("unknown mutation operation")
		}
	}

	// Remove any create or touch relationships that are being deleted.
	for key := range deleteRels {
		if _, exists := createRels[key]; exists {
			delete(createRels, key)
		}

		if _, exists := touchRels[key]; exists {
			delete(touchRels, key)
		}
	}

	return maps.Values(createRels), maps.Values(touchRels), maps.Values(deleteRels), nil
}

// DeleteRelationships implements datastore.ReadWriteTransaction.
func (*sqliteReaderWriter) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	panic("unimplemented")
}

// WriteCaveats implements datastore.ReadWriteTransaction.
func (rw *sqliteReaderWriter) WriteCaveats(ctx context.Context, caveats []*corev1.CaveatDefinition) error {
	if len(caveats) == 0 {
		return nil
	}

	// Iterate over the caveats to build the necessary SQL clauses.
	insertValues := make([]any, 0, len(caveats)*3)
	names := make([]string, 0, len(caveats))
	for _, caveat := range caveats {
		insertValues = append(insertValues, caveat.Name, caveat.SerializedExpression, rw.txID)
		names = append(names, caveat.Name)
	}

	// Mark the living caveats that will be replaced as dead.
	//
	// This doesn't reuse logic from DeleteCaveats() because not all caveats
	// may already exist.
	if err := rw.exec(markDeletedUpdate("caveat", rw.txID).Where(sq.Eq{"name": names}), nil); err != nil {
		return err
	}

	// Insert the new namespaces.
	return rw.exec(
		sq.Insert("caveat").
			Columns("name", "expression", "created_txn").
			Values(insertValues...),
		nil,
	)
}

// DeleteCaveats implements datastore.ReadWriteTransaction.
func (rw *sqliteReaderWriter) DeleteCaveats(ctx context.Context, names []string) error {
	// Ensure that the caveats exist.
	if err := rw.exec(
		sq.Select("COUNT(name)").From("caveat").Where(sq.Eq{"name": names}),
		func(stmt *sqlite.Stmt) error {
			count, err := sqlitex.ResultInt64(stmt)
			if err != nil {
				return err
			}
			if count != int64(len(names)) {
				// TODO(jzelinskie): return the name of the missing caveat
				return &datastore.ErrCaveatNameNotFound{}
			}
			return nil
		},
	); err != nil {
		return err
	}

	// Mark the caveats as deleted.
	return rw.exec(markDeletedUpdate("caveat", rw.txID).Where(sq.Eq{"name": names}), nil)
}

// WriteNamespaces implements datastore.ReadWriteTransaction.
func (rw *sqliteReaderWriter) WriteNamespaces(ctx context.Context, namespaces ...*corev1.NamespaceDefinition) error {
	if len(namespaces) == 0 {
		return nil
	}

	// Iterate over the namespaces to build the necessary SQL clauses.
	insertValues := make([]any, 0, len(namespaces)*3)
	names := make([]string, 0, len(namespaces))
	for _, namespace := range namespaces {
		serialized, err := proto.Marshal(namespace)
		if err != nil {
			return err
		}
		insertValues = append(insertValues, namespace.Name, serialized, rw.txID)
		names = append(names, namespace.Name)
	}

	// Mark the living namespaces that will be replaced as dead.
	//
	// This doesn't reuse logic from DeleteNamespaces() because not all
	// namespaces may already exist and there's no need to clean up any
	// relationships.
	if err := rw.exec(markDeletedUpdate("namespace", rw.txID).Where(sq.Eq{"name": names}), nil); err != nil {
		return err
	}

	// Insert the new namespaces.
	return rw.exec(
		sq.Insert("namespace").
			Columns("name", "serialized_config", "created_txn").
			Values(insertValues...),
		nil,
	)
}

// DeleteNamespaces implements datastore.ReadWriteTransaction.
func (rw *sqliteReaderWriter) DeleteNamespaces(ctx context.Context, namespaces ...string) error {
	// Ensure that the namespaces exist.
	if err := rw.exec(
		sq.Select("COUNT(name)").From("namespace").Where(sq.Eq{"name": namespaces}),
		func(stmt *sqlite.Stmt) error {
			count, err := sqlitex.ResultInt64(stmt)
			if err != nil {
				return err
			}
			if count != int64(len(namespaces)) {
				// TODO(jzelinskie): return the name of the missing namespace
				return &datastore.ErrNamespaceNotFound{}
			}
			return nil
		},
	); err != nil {
		return err
	}

	// Mark the namespaces as deleted.
	if err := rw.exec(markDeletedUpdate("namespace", rw.txID).Where(sq.Eq{"name": namespaces}), nil); err != nil {
		return err
	}

	relClauses := make([]sq.Sqlizer, 0, len(namespaces)*2)
	for _, namespace := range namespaces {
		relClauses = append(relClauses,
			sq.Eq{"object_type": namespace},
			sq.Eq{"subject_type": namespace},
		)
	}

	// Mark any relationships as deleted.
	return rw.exec(markDeletedUpdate("relationship", rw.txID).Where(sq.Or(relClauses)), nil)
}

func markDeletedUpdate(table string, txID int64) sq.UpdateBuilder {
	return sq.Update(table).
		Set("deleted_txn", txID).
		Where(sq.Eq{"deleted_txn": livingTxID})
}

func (rw *sqliteReaderWriter) exec(b sq.Sqlizer, fn execResultFunc) error {
	query, args, err := b.ToSql()
	if err != nil {
		return spiceerrors.MustBugf("failed to generate SQL: %w", err)
	}
	return sqlitex.Execute(rw.conn(), query, &sqlitex.ExecOptions{
		Args:       args,
		ResultFunc: fn,
	})
}

var _ datastore.ReadWriteTransaction = (*sqliteReaderWriter)(nil)
