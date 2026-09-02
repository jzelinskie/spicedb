package query

import (
	"fmt"
	"io"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

func init() {
	MustRegisterIterator(IteratorSpec{
		Type: ArrowIteratorType,
		Name: "Arrow",
		ConstructWithArgs: func(_ *IteratorArgs, subs []Iterator, key CanonicalKey) (Iterator, error) {
			if len(subs) != 2 {
				return nil, fmt.Errorf("ArrowIterator requires exactly 2 subiterators, got %d", len(subs))
			}
			arrow := NewArrowIterator(subs[0], subs[1])
			arrow.canonicalKey = key
			return arrow, nil
		},
		Deserialize: deserializeArrow,
	})
}

// arrowDirection specifies which direction to execute the arrow check
type arrowDirection int

const (
	// leftToRight executes IterSubjects on left, Check on right
	leftToRight arrowDirection = iota
	// rightToLeft executes IterResources on right, Check on left
	rightToLeft
)

// ArrowIterator is an iterator that represents the set of paths that
// follow from a walk in the graph.
//
// Ex: `folder->owner` and `left->right`
type ArrowIterator struct {
	left          Iterator
	right         Iterator
	direction     arrowDirection // execution direction
	isSchemaArrow bool           // true for schema arrows (relation->permission), false for subrelation arrows
	canonicalKey  CanonicalKey
}

var _ Iterator = &ArrowIterator{}

func NewArrowIterator(left, right Iterator) *ArrowIterator {
	return &ArrowIterator{
		left:          left,
		right:         right,
		direction:     leftToRight,
		isSchemaArrow: false, // default to subrelation arrow
	}
}

func NewSchemaArrow(left, right Iterator) *ArrowIterator {
	return &ArrowIterator{
		left:          left,
		right:         right,
		direction:     leftToRight,
		isSchemaArrow: true,
	}
}

func (a *ArrowIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// There are three major strategies:
	// - IterSubjects on the left, Check on the right
	// - IterResources on the right, Check on the left
	// - IterSubjects on left, IterResources on right, and intersect the two iterators here (especially if they are known to be sorted)
	//
	// But for now, we cover the first two. When BatchedArrows is set the
	// per-element Check loop is replaced by a single CheckMany call so the
	// executor (e.g. DispatchExecutor) can collapse fanout into one RPC.
	switch a.direction {
	case leftToRight:
		if ctx.BatchedArrows {
			return a.checkLeftToRightBatch(ctx, resource, subject)
		}
		return a.checkLeftToRight(ctx, resource, subject)
	case rightToLeft:
		if ctx.BatchedArrows {
			return a.checkRightToLeftBatch(ctx, resource, subject)
		}
		return a.checkRightToLeft(ctx, resource, subject)
	default:
		return nil, spiceerrors.MustBugf("unknown arrow direction: %d", a.direction)
	}
}

// checkLeftToRight implements the left-to-right strategy:
// IterSubjects on left for the resource, then Check on right for each left subject.
// Returns the first found combined path (OR-merged if multiple left paths lead to the same subject).
func (a *ArrowIterator) checkLeftToRight(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow check (left-to-right) for resource %s:%s", resource.ObjectType, resource.ObjectID)
	}

	subit, err := ctx.IterSubjects(a.left, resource, NoObjectFilter())
	if err != nil {
		return nil, err
	}

	var result *Path
	leftPathCount := 0
	for leftPath, err := range subit {
		if err != nil {
			return nil, err
		}
		leftPathCount++

		// If the left side returned a wildcard (e.g., folder:*), we can't use it as a
		// resource for the right side. Instead, invert: call IterResources on the right
		// with the target subject to find any intermediate of the matching type.
		if leftPath.Subject.ObjectID == tuple.PublicWildcard {
			if ctx.shouldTrace() {
				ctx.TraceStep(a, "left returned wildcard %s:*, using IterResources inversion", leftPath.Subject.ObjectType)
			}
			rightSeq, err := ctx.IterResources(a.right, subject, ObjectType{Type: leftPath.Subject.ObjectType})
			if err != nil {
				return nil, err
			}
			for rightPath, err := range rightSeq {
				if err != nil {
					return nil, err
				}
				// Any matching intermediate means the wildcard left is satisfied.
				combined := combineArrowPaths(leftPath, rightPath)
				if combined.Caveat == nil {
					return combined, nil
				}
				result, err = result.MergeOr(combined)
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(a, "checking right side for left subject %s:%s", leftPath.Subject.ObjectType, leftPath.Subject.ObjectID)
		}

		rightPath, err := ctx.Check(a.right, GetObject(leftPath.Subject), subject)
		if err != nil {
			return nil, err
		}

		if rightPath == nil {
			continue
		}

		combined := combineArrowPaths(leftPath, rightPath)
		if combined.Caveat == nil {
			return combined, nil
		}

		result, err = result.MergeOr(combined)
		if err != nil {
			return nil, err
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow (left-to-right) completed: %d left paths, found=%v", leftPathCount, result != nil)
	}
	return result, nil
}

// checkRightToLeft implements the right-to-left strategy:
// IterResources on right to get candidate intermediates, then Check on left for the resource.
func (a *ArrowIterator) checkRightToLeft(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow check (right-to-left) for resource %s:%s, subject %s:%s",
			resource.ObjectType, resource.ObjectID, subject.ObjectType, subject.ObjectID)
	}

	// Start from the right side with the target subject to get candidate intermediates
	rightSeq, err := ctx.IterResources(a.right, subject, NoObjectFilter())
	if err != nil {
		return nil, err
	}

	var result *Path
	rightPathCount := 0
	for rightPath, err := range rightSeq {
		if err != nil {
			return nil, err
		}
		rightPathCount++

		// rightPath.Resource is an intermediate object from the right side.
		// Check if our input resource connects to this intermediate via the left side.
		// The subject relation must match what the left side stores, which differs
		// between schema arrows (left stores subjects with Ellipsis) and subrelation
		// arrows (left stores subjects with a specific subject_relation, e.g. `member`).
		// Derive it from the left's SubjectTypes for the intermediate's type.
		intermediateAsSubject := ObjectAndRelation{
			ObjectType: rightPath.Resource.ObjectType,
			ObjectID:   rightPath.Resource.ObjectID,
			Relation:   a.intermediateSubjectRelation(rightPath.Resource.ObjectType),
		}

		leftPath, err := ctx.Check(a.left, resource, intermediateAsSubject)
		if err != nil {
			return nil, err
		}

		if leftPath == nil {
			continue
		}

		combined := combineArrowPaths(leftPath, rightPath)
		if combined.Caveat == nil {
			return combined, nil
		}

		result, err = result.MergeOr(combined)
		if err != nil {
			return nil, err
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(a, "left matched intermediate %s:%s", intermediateAsSubject.ObjectType, intermediateAsSubject.ObjectID)
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow (right-to-left) completed: %d right paths, found=%v", rightPathCount, result != nil)
	}
	return result, nil
}

// intermediateSubjectRelation returns the subject_relation that the LEFT side's
// datastore expects for subjects of the given type. Schema arrows store
// subjects with Ellipsis (no subject_relation in the schema's allowed type),
// while subrelation arrows store subjects with a specific relation (e.g.
// team:foo#member). Passing the wrong relation silently misses datastore rows.
//
// Implementation derives the expected relation from a.left.SubjectTypes(),
// falling back to Ellipsis if the type isn't reflected there (e.g. constructed
// iterator without metadata).
func (a *ArrowIterator) intermediateSubjectRelation(intermediateType string) string {
	subjectTypes, err := a.left.SubjectTypes()
	if err != nil {
		return tuple.Ellipsis
	}
	for _, st := range subjectTypes {
		if st.Type != intermediateType {
			continue
		}
		if st.Subrelation == "" {
			return tuple.Ellipsis
		}
		return st.Subrelation
	}
	return tuple.Ellipsis
}

// checkLeftToRightBatch is the batched variant of checkLeftToRight: it drains
// all left subjects first, then issues a single CheckManyResources call against
// the right side. Wildcards still fall back to the per-element IterResources
// inversion path because they cannot be used as a concrete resource on the right.
func (a *ArrowIterator) checkLeftToRightBatch(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow check (left-to-right, batched) for resource %s:%s", resource.ObjectType, resource.ObjectID)
	}

	subit, err := ctx.IterSubjects(a.left, resource, NoObjectFilter())
	if err != nil {
		return nil, err
	}
	leftPaths, err := CollectAll(subit)
	if err != nil {
		return nil, err
	}

	var result *Path
	concreteLeft := make([]*Path, 0, len(leftPaths))
	concreteRes := make([]Object, 0, len(leftPaths))
	for _, leftPath := range leftPaths {
		// Wildcards: invert to IterResources, like the unbatched path.
		if leftPath.Subject.ObjectID == tuple.PublicWildcard {
			rightSeq, err := ctx.IterResources(a.right, subject, ObjectType{Type: leftPath.Subject.ObjectType})
			if err != nil {
				return nil, err
			}
			for rightPath, err := range rightSeq {
				if err != nil {
					return nil, err
				}
				combined := combineArrowPaths(leftPath, rightPath)
				if combined.Caveat == nil {
					return combined, nil
				}
				result, err = result.MergeOr(combined)
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		concreteLeft = append(concreteLeft, leftPath)
		concreteRes = append(concreteRes, GetObject(leftPath.Subject))
	}

	if len(concreteRes) > 0 {
		rightPaths, err := ctx.CheckManyResources(a.right, concreteRes, subject)
		if err != nil {
			return nil, err
		}
		for i, rightPath := range rightPaths {
			if rightPath == nil {
				continue
			}
			combined := combineArrowPaths(concreteLeft[i], rightPath)
			if combined.Caveat == nil {
				return combined, nil
			}
			result, err = result.MergeOr(combined)
			if err != nil {
				return nil, err
			}
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow (left-to-right, batched) completed: %d concrete, found=%v", len(concreteLeft), result != nil)
	}
	return result, nil
}

// checkRightToLeftBatch is the batched variant of checkRightToLeft: it drains
// all right resources first, then issues a single CheckManySubjects call against
// the left side using the right resources (with ellipsis relation) as subjects.
func (a *ArrowIterator) checkRightToLeftBatch(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow check (right-to-left, batched) for resource %s:%s, subject %s:%s",
			resource.ObjectType, resource.ObjectID, subject.ObjectType, subject.ObjectID)
	}

	rightSeq, err := ctx.IterResources(a.right, subject, NoObjectFilter())
	if err != nil {
		return nil, err
	}
	rightPaths, err := CollectAll(rightSeq)
	if err != nil {
		return nil, err
	}
	if len(rightPaths) == 0 {
		return nil, nil
	}

	intermediates := make([]ObjectAndRelation, len(rightPaths))
	for i, rightPath := range rightPaths {
		intermediates[i] = ObjectAndRelation{
			ObjectType: rightPath.Resource.ObjectType,
			ObjectID:   rightPath.Resource.ObjectID,
			Relation:   a.intermediateSubjectRelation(rightPath.Resource.ObjectType),
		}
	}

	leftPaths, err := ctx.CheckManySubjects(a.left, resource, intermediates)
	if err != nil {
		return nil, err
	}

	var result *Path
	for i, leftPath := range leftPaths {
		if leftPath == nil {
			continue
		}
		combined := combineArrowPaths(leftPath, rightPaths[i])
		if combined.Caveat == nil {
			return combined, nil
		}
		result, err = result.MergeOr(combined)
		if err != nil {
			return nil, err
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow (right-to-left, batched) completed: %d right paths, found=%v", len(rightPaths), result != nil)
	}
	return result, nil
}

// combineArrowPaths combines a left path and right path into a single path for arrow operations.
// The combined path uses the resource and relation from the left path, the subject from the right path,
// and combines caveats from both sides using AND logic.
func combineArrowPaths(leftPath, rightPath *Path) *Path {
	// Combine caveats from both sides using AND logic
	var combinedCaveat *core.CaveatExpression
	switch {
	case leftPath.Caveat != nil && rightPath.Caveat != nil:
		combinedCaveat = caveats.And(leftPath.Caveat, rightPath.Caveat)
	case leftPath.Caveat != nil:
		combinedCaveat = leftPath.Caveat
	case rightPath.Caveat != nil:
		combinedCaveat = rightPath.Caveat
	}

	return &Path{
		Resource:   leftPath.Resource,
		Relation:   leftPath.Relation,
		Subject:    rightPath.Subject,
		Caveat:     combinedCaveat,
		Expiration: combineExpiration(leftPath.Expiration, rightPath.Expiration),
		Integrity:  combineIntegrity(leftPath.Integrity, rightPath.Integrity),
		Metadata:   make(map[string]any),
	}
}

func (a *ArrowIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	return a.IterSubjectsForResourcesImpl(ctx, []Object{resource}, filterSubjectType)
}

// IterSubjectsForResourcesImpl walks the arrow for a batch of resources:
// resources -> left subjects -> right subjects.
//
// Both hops are single batched calls. Unlike a check, an enumeration has no
// early exit — every left subject has to be visited regardless — so draining the
// left side to build the batch costs nothing, and issuing one query for the
// whole set instead of one per element is a pure reduction in round-trips.
//
// Each right path names the intermediate it came from in Path.Resource, which is
// how it is matched back to the left paths that reached that intermediate.
// Several left paths can share an intermediate, so the mapping is one-to-many.
func (a *ArrowIterator) IterSubjectsForResourcesImpl(ctx *Context, resources []Object, filterSubjectType ObjectType) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		if ctx.shouldTrace() {
			ctx.TraceStep(a, "iterating subjects for %d resources", len(resources))
		}

		leftSeq, err := ctx.IterSubjectsForResources(a.left, resources, NoObjectFilter())
		if err != nil {
			yield(nil, err)
			return
		}
		leftPaths, err := CollectAll(leftSeq)
		if err != nil {
			yield(nil, err)
			return
		}

		intermediates := make([]Object, 0, len(leftPaths))
		leftByIntermediate := make(map[Object][]*Path, len(leftPaths))
		for _, leftPath := range leftPaths {
			// If the left side returned a wildcard (e.g., folder:*), we can't use it
			// as a resource for the right side. Skip it — we can't enumerate all
			// concrete subjects reachable through all intermediates of this type without
			// a store-wide query. This matches the traditional dispatch path, which also
			// doesn't expand wildcard tupleset entries through arrows.
			// (The Check path handles this correctly via IterResources inversion.)
			if leftPath.Subject.ObjectID == tuple.PublicWildcard {
				if ctx.shouldTrace() {
					ctx.TraceStep(a, "left returned wildcard %s:*, skipping (cannot follow arrow through wildcard)", leftPath.Subject.ObjectType)
				}
				continue
			}

			intermediate := GetObject(leftPath.Subject)
			if _, seen := leftByIntermediate[intermediate]; !seen {
				intermediates = append(intermediates, intermediate)
			}
			leftByIntermediate[intermediate] = append(leftByIntermediate[intermediate], leftPath)
		}

		if len(intermediates) == 0 {
			if ctx.shouldTrace() {
				ctx.TraceStep(a, "arrow IterSubjects completed: %d left paths, no usable intermediates", len(leftPaths))
			}
			return
		}

		rightSeq, err := ctx.IterSubjectsForResources(a.right, intermediates, filterSubjectType)
		if err != nil {
			yield(nil, err)
			return
		}

		totalResultPaths := 0
		for rightPath, err := range rightSeq {
			if err != nil {
				yield(nil, err)
				return
			}
			for _, leftPath := range leftByIntermediate[rightPath.Resource] {
				totalResultPaths++
				if !yield(combineArrowPaths(leftPath, rightPath), nil) {
					return
				}
			}
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(a, "arrow IterSubjects completed: %d left paths, %d intermediates, %d total result paths",
				len(leftPaths), len(intermediates), totalResultPaths)
		}
	}, nil
}

func (a *ArrowIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	return a.IterResourcesForSubjectsImpl(ctx, []ObjectAndRelation{subject}, filterResourceType)
}

// IterResourcesForSubjectsImpl walks the arrow backwards for a batch of
// subjects: subjects -> right resources -> left resources.
//
// Both hops are single batched calls, for the same reason as the forward walk:
// an enumeration has no early exit, so draining the right side to build the
// batch costs nothing and one query beats one per intermediate. This is what
// keeps a nested arrow from multiplying its inner walk by the outer fan-out.
//
// A schema arrow queries the left with both the intermediate's own relation and
// ellipsis, because relationships may be stored either way; a subrelation arrow
// queries only with the subrelation the left side expects. Each left path names
// the intermediate it came from in Path.Subject, which is how it is matched back
// to the right paths that reached it.
func (a *ArrowIterator) IterResourcesForSubjectsImpl(ctx *Context, subjects []ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		if ctx.shouldTrace() {
			ctx.TraceStep(a, "iterating resources for %d subjects", len(subjects))
		}

		rightSeq, err := ctx.IterResourcesForSubjects(a.right, subjects, NoObjectFilter())
		if err != nil {
			yield(nil, err)
			return
		}
		rightPaths, err := CollectAll(rightSeq)
		if err != nil {
			yield(nil, err)
			return
		}

		// Note: We used to filter self-edges here, but self-edges from Alias represent valid identity checks
		// (e.g., team:first#member accessing team:first via member). Removing the filter allows these
		// identity relationships to propagate through arrows correctly.

		// Group the right paths by the intermediate-as-subject the left side must
		// be queried with. Several right paths can share an intermediate, so the
		// mapping is one-to-many.
		intermediates := make([]ObjectAndRelation, 0, len(rightPaths))
		rightByIntermediate := make(map[ObjectAndRelation][]*Path, len(rightPaths))
		addIntermediate := func(oar ObjectAndRelation, rightPath *Path) {
			if _, seen := rightByIntermediate[oar]; !seen {
				intermediates = append(intermediates, oar)
			}
			rightByIntermediate[oar] = append(rightByIntermediate[oar], rightPath)
		}

		leftSubjectTypes, err := a.left.SubjectTypes()
		if err != nil {
			yield(nil, err)
			return
		}

		for _, rightPath := range rightPaths {
			rightResourceAsSubject := rightPath.ResourceOAR()

			if a.isSchemaArrow {
				// Schema arrow: query with both the specific relation and ellipsis.
				addIntermediate(rightResourceAsSubject, rightPath)
				addIntermediate(ObjectAndRelation{
					ObjectType: rightResourceAsSubject.ObjectType,
					ObjectID:   rightResourceAsSubject.ObjectID,
					Relation:   tuple.Ellipsis,
				}, rightPath)
				continue
			}

			// Subrelation arrow: query with the left iterator's expected subrelation.
			// (For subrelation arrows there should be exactly one expected type.)
			if len(leftSubjectTypes) > 0 && leftSubjectTypes[0].Type == rightResourceAsSubject.ObjectType {
				addIntermediate(ObjectAndRelation{
					ObjectType: rightResourceAsSubject.ObjectType,
					ObjectID:   rightResourceAsSubject.ObjectID,
					Relation:   leftSubjectTypes[0].Subrelation,
				}, rightPath)
			}
		}

		if len(intermediates) == 0 {
			if ctx.shouldTrace() {
				ctx.TraceStep(a, "arrow IterResources completed: %d right paths, no usable intermediates", len(rightPaths))
			}
			return
		}

		leftSeq, err := ctx.IterResourcesForSubjects(a.left, intermediates, filterResourceType)
		if err != nil {
			yield(nil, err)
			return
		}

		totalResultPaths := 0
		for leftPath, err := range leftSeq {
			if err != nil {
				yield(nil, err)
				return
			}
			for _, rightPath := range rightByIntermediate[leftPath.Subject] {
				totalResultPaths++
				if !yield(combineArrowPaths(leftPath, rightPath), nil) {
					return
				}
			}
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(a, "arrow IterResources completed: %d right paths, %d intermediates, %d total result paths",
				len(rightPaths), len(intermediates), totalResultPaths)
		}
	}, nil
}

func (a *ArrowIterator) Clone() Iterator {
	return &ArrowIterator{
		canonicalKey:  a.canonicalKey,
		left:          a.left.Clone(),
		right:         a.right.Clone(),
		direction:     a.direction,     // preserve direction
		isSchemaArrow: a.isSchemaArrow, // preserve arrow type
	}
}

func (a *ArrowIterator) Explain() Explain {
	var kind string
	switch a.direction {
	case rightToLeft:
		kind = "RTL"
	case leftToRight:
		kind = "LTR"
	}
	return Explain{
		Name:       "Arrow",
		Info:       fmt.Sprintf("Arrow(%s)", kind),
		SubExplain: []Explain{a.left.Explain(), a.right.Explain()},
	}
}

func (a *ArrowIterator) Subiterators() []Iterator {
	return []Iterator{a.left, a.right}
}

func (a *ArrowIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &ArrowIterator{
		canonicalKey:  a.canonicalKey,
		left:          newSubs[0],
		right:         newSubs[1],
		direction:     a.direction,
		isSchemaArrow: a.isSchemaArrow,
	}, nil
}

func (a *ArrowIterator) CanonicalKey() CanonicalKey {
	return a.canonicalKey
}

func (a *ArrowIterator) ResourceType() ([]ObjectType, error) {
	// Arrow's resources come from the left side
	return a.left.ResourceType()
}

func (a *ArrowIterator) SubjectTypes() ([]ObjectType, error) {
	// Arrow's subjects come from the right side
	return a.right.SubjectTypes()
}

const (
	arrowFlagSchemaArrow = iota
	arrowFlagRightToLeft
)

func (a *ArrowIterator) Serialize(w io.Writer) error {
	return SerializeWithHeader(w, ArrowIteratorType, a.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, arrowFlagSchemaArrow, a.isSchemaArrow)
		setFlag(&flags, arrowFlagRightToLeft, a.direction == rightToLeft)
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		if err := a.left.Serialize(buf); err != nil {
			return fmt.Errorf("left: %w", err)
		}
		if err := a.right.Serialize(buf); err != nil {
			return fmt.Errorf("right: %w", err)
		}
		return nil
	})
}

func deserializeArrow(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("arrow flags: %w", err)
	}
	subs, err := readNSubs(br, 2, dctx)
	if err != nil {
		return nil, err
	}
	a := &ArrowIterator{
		left:          subs[0],
		right:         subs[1],
		isSchemaArrow: hasFlag(flags, arrowFlagSchemaArrow),
		direction:     leftToRight,
		canonicalKey:  key,
	}
	if hasFlag(flags, arrowFlagRightToLeft) {
		a.direction = rightToLeft
	}
	return a, nil
}
