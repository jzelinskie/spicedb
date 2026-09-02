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

// documentsWithViewers builds N documents, each with one distinct viewer.
func documentsWithViewers(n int) ([]tuple.Relationship, []Object, []ObjectAndRelation) {
	rels := make([]tuple.Relationship, 0, n)
	resources := make([]Object, 0, n)
	subjects := make([]ObjectAndRelation, 0, n)
	for i := range n {
		rels = append(rels, tuple.MustParse(fmt.Sprintf("document:doc%d#viewer@user:user%d", i, i)))
		resources = append(resources, NewObject("document", fmt.Sprintf("doc%d", i)))
		subjects = append(subjects, NewObject("user", fmt.Sprintf("user%d", i)).WithEllipses())
	}
	return rels, resources, subjects
}

// TestIterSubjectsForResourcesDoesNotScaleWithResourceCount is the round-trip
// invariant for the iteration axis: enumerating subjects for many resources
// must cost the same number of enumeration queries as enumerating for one.
//
// The assertion is on ReaderCounts.Subjects rather than the total because the
// alias self-edge probe is a separate, still-unbatched query — one existence
// probe per resource per alias level. That is asserted explicitly below so the
// remaining gap is recorded rather than hidden; when the probe is batched or
// decided statically from the schema, this test should tighten to the total.
func TestIterSubjectsForResourcesDoesNotScaleWithResourceCount(t *testing.T) {
	require := require.New(t)

	countsFor := func(count int) (ReaderCounts, []*Path) {
		rels, resources, _ := documentsWithViewers(count)
		it, reader, opts := newBatchCheckFixture(t, rels)

		pathSeq, err := NewLocalContext(t.Context(), opts...).
			IterSubjectsForResources(it, resources, NoObjectFilter())
		require.NoError(err)
		paths, err := CollectAll(pathSeq)
		require.NoError(err)
		return reader.Counts(), paths
	}

	one, _ := countsFor(1)
	many, paths := countsFor(8)

	require.Equal(1, one.Subjects, "one resource takes one enumeration query")
	require.Equal(one.Subjects, many.Subjects,
		"a batch of 8 resources must take the same number of enumeration queries as a batch of 1")

	// Not yet batched: the alias self-edge probe is still one query per resource
	// per alias level. Recorded so a change in either direction is visible.
	require.Equal(8*one.ExistenceProbes, many.ExistenceProbes,
		"self-edge probes still scale with the batch; see AliasIterator.shouldIncludeSelfEdge")

	subjectsByResource := map[string]string{}
	for _, path := range paths {
		subjectsByResource[path.Resource.ObjectID] = path.Subject.ObjectID
	}
	require.Len(subjectsByResource, 8, "every resource must be represented")
	for i := range 8 {
		require.Equal(fmt.Sprintf("user%d", i), subjectsByResource[fmt.Sprintf("doc%d", i)],
			"paths must stay attributed to the resource they came from")
	}
}

// TestIterResourcesForSubjectsDoesNotScaleWithSubjectCount is the subject-axis
// counterpart.
func TestIterResourcesForSubjectsDoesNotScaleWithSubjectCount(t *testing.T) {
	require := require.New(t)

	queriesFor := func(count int) (int, []*Path) {
		rels, _, subjects := documentsWithViewers(count)
		it, reader, opts := newBatchCheckFixture(t, rels)

		pathSeq, err := NewLocalContext(t.Context(), opts...).
			IterResourcesForSubjects(it, subjects, NoObjectFilter())
		require.NoError(err)
		paths, err := CollectAll(pathSeq)
		require.NoError(err)
		return reader.Queries(), paths
	}

	oneSubject, _ := queriesFor(1)
	manySubjects, paths := queriesFor(8)

	require.Equal(oneSubject, manySubjects,
		"a batch of 8 subjects must cost the same as a batch of 1")

	resourcesBySubject := map[string]string{}
	for _, path := range paths {
		resourcesBySubject[path.Subject.ObjectID] = path.Resource.ObjectID
	}
	require.Len(resourcesBySubject, 8, "every subject must be represented")
	for i := range 8 {
		require.Equal(fmt.Sprintf("doc%d", i), resourcesBySubject[fmt.Sprintf("user%d", i)],
			"paths must stay attributed to the subject they came from")
	}
}
