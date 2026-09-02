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

// BatchSubjectsWalker is implemented by iterators that can enumerate subjects
// from several resources in fewer datastore round-trips than the equivalent loop
// over IterSubjectsImpl.
//
// Like BatchIterator it is optional, asked for with a type assertion, with a
// per-resource loop as the fallback. Unlike a batched check, the result is a
// single stream rather than a parallel slice: callers attribute each path back
// to its starting point through Path.Resource, which every iterator preserves.
//
// The two axes are separate interfaces on purpose. An iterator may be able to
// batch one and not the other, and a single interface requiring both would
// silently fall back to the per-element loop for an iterator that implemented
// only the axis it can actually batch.
type BatchSubjectsWalker interface {
	// IterSubjectsForResourcesImpl enumerates the subjects of every resource in
	// resources.
	IterSubjectsForResourcesImpl(ctx *Context, resources []Object, filterSubjectType ObjectType) (PathSeq, error)
}

// BatchResourcesWalker is the subject-axis counterpart of BatchSubjectsWalker:
// iterators that can enumerate resources from several subjects at once. Paths
// are attributed back through Path.Subject.
type BatchResourcesWalker interface {
	// IterResourcesForSubjectsImpl enumerates the resources of every subject in
	// subjects.
	IterResourcesForSubjectsImpl(ctx *Context, subjects []ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error)
}

// IterSubjectsForResourcesOn enumerates the subjects reachable from every
// resource in resources, as one stream.
//
// As with CheckManySubjectsOn, this is the single place the batch-or-loop
// decision is made: a BatchWalkIterator receives the whole slice, anything else
// is walked one resource at a time.
func IterSubjectsForResourcesOn(ctx *Context, it Iterator, resources []Object, filterSubjectType ObjectType) (PathSeq, error) {
	if batch, ok := it.(BatchSubjectsWalker); ok {
		return batch.IterSubjectsForResourcesImpl(ctx, resources, filterSubjectType)
	}

	return iterSubjectsPerResource(ctx, it, resources, filterSubjectType)
}

// iterSubjectsPerResource is the unbatched walk: one IterSubjectsImpl call per
// resource, concatenated. It backs the fallback in IterSubjectsForResourcesOn
// and is also used by batch-capable iterators for the cases they cannot batch,
// such as a paginated query whose cursor cannot be shared across resources.
func iterSubjectsPerResource(ctx *Context, it Iterator, resources []Object, filterSubjectType ObjectType) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		for _, resource := range resources {
			pathSeq, err := it.IterSubjectsImpl(ctx, resource, filterSubjectType)
			if err != nil {
				yield(nil, err)
				return
			}
			for path, err := range FilterSubjectsByType(pathSeq, filterSubjectType) {
				if !yield(path, err) {
					return
				}
			}
		}
	}, nil
}

// IterResourcesForSubjectsOn enumerates the resources reachable from every
// subject in subjects, as one stream. It is the subject-axis counterpart of
// IterSubjectsForResourcesOn.
func IterResourcesForSubjectsOn(ctx *Context, it Iterator, subjects []ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	if batch, ok := it.(BatchResourcesWalker); ok {
		return batch.IterResourcesForSubjectsImpl(ctx, subjects, filterResourceType)
	}

	return iterResourcesPerSubject(ctx, it, subjects, filterResourceType)
}

// iterResourcesPerSubject is the unbatched walk, the subject-axis counterpart of
// iterSubjectsPerResource.
func iterResourcesPerSubject(ctx *Context, it Iterator, subjects []ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		for _, subject := range subjects {
			pathSeq, err := it.IterResourcesImpl(ctx, subject, filterResourceType)
			if err != nil {
				yield(nil, err)
				return
			}
			for path, err := range FilterResourcesByType(pathSeq, filterResourceType) {
				if !yield(path, err) {
					return
				}
			}
		}
	}, nil
}
