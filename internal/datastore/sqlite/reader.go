package sqlite

import (
	"context"
	"fmt"
	"strings"

	"crawshaw.io/sqlite"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type sqliteReader struct {
	conn     *sqlite.Conn
	snapshot *datastore.Revision
}

func (r *sqliteReader) queryLivingNamespaces(names ...string) ([]datastore.RevisionedNamespace, error) {
	var nameFilter string
	if len(names) > 0 {
		nameFilter = fmt.Sprintf(" AND name IN (%s)", strings.Join(names, ", "))
	}

	stmt := r.conn.Prep(`
		SELECT serialized_config, created_txn
		FROM namespace
		WHERE deleted_txn = $living_txid
	` + nameFilter + ";")
	stmt.SetInt64("$living_txn", livingTxID)

	return scanNamespaces(stmt)
}

func (r *sqliteReader) queryNamespacesAtSnapshot(names ...string) ([]datastore.RevisionedNamespace, error) {
	var nameFilter string
	if len(names) > 0 {
		nameFilter = fmt.Sprintf(" AND name IN (%s)", strings.Join(names, ", "))
	}

	stmt := r.conn.Prep(`
		SELECT serialized_config, created_txn
		FROM namespace
		WHERE created_txn <= $txid
		AND (
			deleted_txn = $living_txn
			OR deleted_txn > $txid
		)
	` + nameFilter + ";")
	stmt.SetInt64("$living_txn", livingTxID)
	stmt.SetInt64("$txid", revToTx(*r.snapshot))

	return scanNamespaces(stmt)
}

func scanNamespaces(stmt *sqlite.Stmt) (namespaces []datastore.RevisionedNamespace, err error) {
	var buf []byte
	for {
		if hasRow, err := stmt.Step(); err != nil {
			return nil, err
		} else if !hasRow {
			break
		}

		namespace := &corev1.NamespaceDefinition{}
		stmt.GetBytes("serialized_config", buf)
		if err := namespace.UnmarshalVT(buf); err != nil {
			return nil, err
		}
		clear(buf)

		rev, err := revFromTx(stmt.GetInt64("created_txn"))
		if err != nil {
			return nil, err
		}

		namespaces = append(namespaces, datastore.RevisionedNamespace{
			Definition:          namespace,
			LastWrittenRevision: rev,
		})

	}
	return namespaces, nil
}

func (r *sqliteReader) queryLivingCaveats(names ...string) ([]datastore.RevisionedCaveat, error) {
	var nameFilter string
	if len(names) > 0 {
		nameFilter = fmt.Sprintf(" WHERE name IN (%s)", strings.Join(names, ", "))
	}

	stmt := r.conn.Prep(`
		SELECT expression, created_txn
		FROM caveat
		WHERE deleted_txn = $living_txid
		ORDER BY name
	` + nameFilter + ";")
	stmt.SetInt64("$living_txn", livingTxID)

	return scanCaveats(stmt)
}

func (r *sqliteReader) queryCaveatsAtSnapshot(names ...string) ([]datastore.RevisionedCaveat, error) {
	var nameFilter string
	if len(names) > 0 {
		nameFilter = fmt.Sprintf(" AND name IN (%s)", strings.Join(names, ", "))
	}

	stmt := r.conn.Prep(`
		SELECT expression, created_txn
		FROM caveat
		WHERE created_txn <= $txid
		AND (
			deleted_txn = $living_txn
			OR deleted_txn > $txid
		)
	` + nameFilter + " ORDER BY name;")
	stmt.SetInt64("$living_txn", livingTxID)
	stmt.SetInt64("$txid", revToTx(*r.snapshot))

	return scanCaveats(stmt)
}

func scanCaveats(stmt *sqlite.Stmt) (caveats []datastore.RevisionedCaveat, err error) {
	var buf []byte
	for {
		if hasRow, err := stmt.Step(); err != nil {
			return nil, err
		} else if !hasRow {
			break
		}

		caveat := &corev1.CaveatDefinition{}
		stmt.GetBytes("expression", buf)
		if err := caveat.UnmarshalVT(buf); err != nil {
			return nil, err
		}
		clear(buf)

		rev, err := revFromTx(stmt.GetInt64("created_txn"))
		if err != nil {
			return nil, err
		}

		caveats = append(caveats, datastore.RevisionedCaveat{
			Definition:          caveat,
			LastWrittenRevision: rev,
		})

	}
	return caveats, nil
}

// ListAllCaveats implements datastore.Reader.
func (r *sqliteReader) ListAllCaveats(_ context.Context) ([]datastore.RevisionedDefinition[*corev1.CaveatDefinition], error) {
	if r.snapshot != nil {
		return r.queryCaveatsAtSnapshot()
	}
	return r.queryLivingCaveats()
}

// LookupCaveatsWithNames implements datastore.Reader.
func (r *sqliteReader) LookupCaveatsWithNames(_ context.Context, names []string) ([]datastore.RevisionedDefinition[*corev1.CaveatDefinition], error) {
	if len(names) == 0 {
		return nil, nil
	}

	if r.snapshot != nil {
		return r.queryCaveatsAtSnapshot(names...)
	}
	return r.queryLivingCaveats(names...)
}

// ListAllNamespaces implements datastore.Reader.
func (r *sqliteReader) ListAllNamespaces(_ context.Context) ([]datastore.RevisionedDefinition[*corev1.NamespaceDefinition], error) {
	if r.snapshot != nil {
		return r.queryNamespacesAtSnapshot()
	}
	return r.queryLivingNamespaces()
}

// LookupNamespacesWithNames implements datastore.Reader.
func (r *sqliteReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if len(nsNames) == 0 {
		return nil, nil
	}

	if r.snapshot != nil {
		return r.queryNamespacesAtSnapshot(nsNames...)
	}
	return r.queryLivingNamespaces(nsNames...)
}

// QueryRelationships implements datastore.Reader.
func (*sqliteReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, options ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	panic("unimplemented")
}

// ReadCaveatByName implements datastore.Reader.
func (*sqliteReader) ReadCaveatByName(ctx context.Context, name string) (caveat *corev1.CaveatDefinition, lastWritten datastore.Revision, err error) {
	panic("unimplemented")
}

// ReadNamespaceByName implements datastore.Reader.
func (*sqliteReader) ReadNamespaceByName(ctx context.Context, nsName string) (ns *corev1.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	panic("unimplemented")
}

// ReverseQueryRelationships implements datastore.Reader.
func (*sqliteReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, options ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	panic("unimplemented")
}

var _ datastore.Reader = (*sqliteReader)(nil)
