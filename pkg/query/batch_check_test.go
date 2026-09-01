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

const batchCheckSchema = `
definition user {}

definition document {
	relation viewer: user
	permission view = viewer
}
`

// newBatchCheckFixture builds an iterator for document#view over the given
// relationships and returns it with a counting reader wrapped context.
func newBatchCheckFixture(t *testing.T, relationships []tuple.Relationship) (Iterator, *CountingReader, []ContextOption) {
	t.Helper()
	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, batchCheckSchema, relationships)

	dsSchema, err := ReadSchema(t.Context(), ds, revision)
	require.NoError(err)

	canonicalOutline, err := BuildOutlineFromSchema(dsSchema, "document", "view")
	require.NoError(err)
	it, err := canonicalOutline.Compile()
	require.NoError(err)

	reader := NewCountingReader(
		NewQueryDatastoreReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)),
	)
	return it, reader, []ContextOption{WithReader(reader)}
}

// TestCheckManySubjectsIssuesOneQuery pins the round-trip cost of a batched
// check: many subjects against one resource must reach the datastore as a
// single query with all subject IDs in the filter, not one query per subject.
func TestCheckManySubjectsIssuesOneQuery(t *testing.T) {
	require := require.New(t)

	it, reader, opts := newBatchCheckFixture(t, []tuple.Relationship{
		tuple.MustParse("document:doc1#viewer@user:alice"),
		tuple.MustParse("document:doc1#viewer@user:carol"),
		tuple.MustParse("document:doc2#viewer@user:bob"),
	})

	subjects := []ObjectAndRelation{
		NewObject("user", "alice").WithEllipses(),
		NewObject("user", "bob").WithEllipses(),
		NewObject("user", "carol").WithEllipses(),
	}

	paths, err := NewLocalContext(t.Context(), opts...).
		CheckManySubjects(it, NewObject("document", "doc1"), subjects)
	require.NoError(err)

	require.Len(paths, 3)
	require.NotNil(paths[0], "alice is a viewer of doc1")
	require.Nil(paths[1], "bob is a viewer of doc2, not doc1")
	require.NotNil(paths[2], "carol is a viewer of doc1")
	require.Equal("alice", paths[0].Subject.ObjectID)
	require.Equal("carol", paths[2].Subject.ObjectID)

	require.Equal(1, reader.Queries(),
		"three subjects against one resource must collapse into a single datastore query")
}

// TestCheckManyResourcesIssuesOneQuery is the resource-axis counterpart: many
// resources against one subject must also collapse into a single query.
func TestCheckManyResourcesIssuesOneQuery(t *testing.T) {
	require := require.New(t)

	it, reader, opts := newBatchCheckFixture(t, []tuple.Relationship{
		tuple.MustParse("document:doc1#viewer@user:alice"),
		tuple.MustParse("document:doc3#viewer@user:alice"),
		tuple.MustParse("document:doc2#viewer@user:bob"),
	})

	resources := []Object{
		NewObject("document", "doc1"),
		NewObject("document", "doc2"),
		NewObject("document", "doc3"),
	}

	paths, err := NewLocalContext(t.Context(), opts...).
		CheckManyResources(it, resources, NewObject("user", "alice").WithEllipses())
	require.NoError(err)

	require.Len(paths, 3)
	require.NotNil(paths[0], "alice is a viewer of doc1")
	require.Nil(paths[1], "alice is not a viewer of doc2")
	require.NotNil(paths[2], "alice is a viewer of doc3")
	require.Equal("doc1", paths[0].Resource.ObjectID)
	require.Equal("doc3", paths[2].Resource.ObjectID)

	require.Equal(1, reader.Queries(),
		"three resources against one subject must collapse into a single datastore query")
}
