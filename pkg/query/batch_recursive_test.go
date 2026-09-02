package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/tuple"
)

const recursiveGroupSchema = `
definition user {}

definition group {
	relation member: user | group#member
}

definition document {
	relation viewer: user | group#member
	permission view = viewer
}
`

// nestedGroupHierarchy gives doc1 a single root group holding `count` subgroups,
// each with one distinct user. The subgroups are discovered together, so they
// form one wide frontier inside a single recursive expansion — exactly the ply
// that should be queried as a batch.
//
// Widening at the root rather than at the document is deliberate: sibling groups
// on the document are fanned out by the enclosing Arrow, one recursive expansion
// each, which is a separate batching problem.
func nestedGroupHierarchy(count int) []tuple.Relationship {
	rels := make([]tuple.Relationship, 0, 2*count+1)
	rels = append(rels, tuple.MustParse("document:doc1#viewer@group:root#member"))
	for i := range count {
		rels = append(rels,
			tuple.MustParse(fmt.Sprintf("group:root#member@group:g%d#member", i)),
			tuple.MustParse(fmt.Sprintf("group:g%d#member@user:u%d", i, i)),
		)
	}
	return rels
}

// TestRecursivePlyDoesNotScaleWithFrontierWidth is the round-trip invariant for
// recursion: a breadth-first ply queries from every object in its frontier, and
// that frontier is already a materialized slice, so widening it must not add
// datastore queries.
//
// The assertion is on ReaderCounts.Subjects; the alias self-edge probe is still
// issued per object and is asserted separately in the iteration-axis tests.
func TestRecursivePlyDoesNotScaleWithFrontierWidth(t *testing.T) {
	require := require.New(t)

	countsFor := func(groupCount int) (ReaderCounts, []*Path) {
		rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
		require.NoError(err)

		ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(
			t, rawDS, recursiveGroupSchema, nestedGroupHierarchy(groupCount))

		dsSchema, err := ReadSchema(t.Context(), ds, revision)
		require.NoError(err)

		canonicalOutline, err := BuildOutlineFromSchema(dsSchema, "document", "view")
		require.NoError(err)
		it, err := canonicalOutline.Compile()
		require.NoError(err)

		reader := NewCountingReader(
			NewQueryDatastoreReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)),
		)
		ctx := NewLocalContext(t.Context(), WithReader(reader), WithMaxRecursionDepth(defaultMaxRecursionDepth))

		pathSeq, err := ctx.IterSubjects(it, NewObject("document", "doc1"), NewType("user"))
		require.NoError(err)
		paths, err := CollectAll(pathSeq)
		require.NoError(err)
		return reader.Counts(), paths
	}

	narrow, _ := countsFor(2)
	wide, paths := countsFor(16)

	require.Equal(narrow.Subjects, wide.Subjects,
		"a 16-wide recursion frontier must take the same enumeration queries as a 2-wide one")

	found := map[string]struct{}{}
	for _, path := range paths {
		found[path.Subject.ObjectID] = struct{}{}
	}
	require.Len(found, 16, "every group's member must be reached")
	for i := range 16 {
		require.Contains(found, fmt.Sprintf("u%d", i))
	}
}
