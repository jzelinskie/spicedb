package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/tuple"
)

const readerTestSchema = `
definition user {}

definition group {
	relation member: user
}

definition document {
	relation viewer: user | group#member
}
`

// TestSubjectExistsAsRelationshipSpecifiesQueryShape covers the alias
// self-edge probe. Every datastore query must declare a query shape: the
// datastore layer uses it to pick or force an index, and the validating
// snapshot reader used by the test fixtures rejects a query without one.
func TestSubjectExistsAsRelationshipSpecifiesQueryShape(t *testing.T) {
	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, readerTestSchema, []tuple.Relationship{
		tuple.MustParse("document:doc1#viewer@group:engineering#member"),
		tuple.MustParse("group:engineering#member@user:alice"),
	})

	reader := NewQueryDatastoreReader(
		datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting),
	)

	exists, err := reader.SubjectExistsAsRelationship(t.Context(), NewObject("group", "engineering"), "member")
	require.NoError(err)
	require.True(exists, "group:engineering appears as a subject with relation member")

	exists, err = reader.SubjectExistsAsRelationship(t.Context(), NewObject("group", "sales"), "member")
	require.NoError(err)
	require.False(exists, "group:sales does not appear as a subject anywhere")
}
