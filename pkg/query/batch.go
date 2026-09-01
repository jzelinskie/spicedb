package query

// BatchIterator is implemented by iterators that can answer a batch of checks
// in fewer datastore round-trips than the equivalent loop over CheckImpl.
//
// It is optional: an executor asks for it with a type assertion and falls back
// to looping CheckImpl when an iterator does not implement it, so a batch
// narrows to a single query wherever the tree supports it and stays correct
// everywhere else. Composite iterators implement it by passing the batch down;
// DatastoreIterator implements it by putting the whole ID set into one filter.
//
// Both methods return a slice parallel to their plural argument, with nil for
// elements that do not match — the same contract as Executor.CheckManySubjects
// and Executor.CheckManyResources.
type BatchIterator interface {
	// CheckManySubjectsImpl tests resource against each subject in subjects.
	CheckManySubjectsImpl(ctx *Context, resource Object, subjects []ObjectAndRelation) ([]*Path, error)

	// CheckManyResourcesImpl tests each resource in resources against subject.
	CheckManyResourcesImpl(ctx *Context, resources []Object, subject ObjectAndRelation) ([]*Path, error)
}

// CheckManySubjectsOn tests resource against each subject in subjects,
// returning a slice parallel to subjects with nil for the ones that do not
// match.
//
// It is the single place that decides whether a batch can be answered as a
// batch: iterators implementing BatchIterator get the whole slice, which is
// what lets it reach the datastore as one query, and everything else falls back
// to a loop over CheckImpl. Both the local executor and the dispatch receiver
// go through here so the decision cannot drift between them.
func CheckManySubjectsOn(ctx *Context, it Iterator, resource Object, subjects []ObjectAndRelation) ([]*Path, error) {
	if batch, ok := it.(BatchIterator); ok {
		return batch.CheckManySubjectsImpl(ctx, resource, subjects)
	}

	out := make([]*Path, len(subjects))
	for i, subject := range subjects {
		path, err := it.CheckImpl(ctx, resource, subject)
		if err != nil {
			return nil, err
		}
		out[i] = path
	}
	return out, nil
}

// CheckManyResourcesOn tests each resource in resources against subject,
// returning a slice parallel to resources. It is the resource-axis counterpart
// of CheckManySubjectsOn.
func CheckManyResourcesOn(ctx *Context, it Iterator, resources []Object, subject ObjectAndRelation) ([]*Path, error) {
	if batch, ok := it.(BatchIterator); ok {
		return batch.CheckManyResourcesImpl(ctx, resources, subject)
	}

	out := make([]*Path, len(resources))
	for i, resource := range resources {
		path, err := it.CheckImpl(ctx, resource, subject)
		if err != nil {
			return nil, err
		}
		out[i] = path
	}
	return out, nil
}
