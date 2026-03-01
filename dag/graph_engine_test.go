package dag

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Builder-shape coverage (graph construction + dependency inference).
type graphAOut struct{}
type graphBOut struct{}
type graphCOut struct{}
type graphDOut struct{}

type graphBIn struct {
	A graphAOut
}

type graphCIn struct {
	A graphAOut
}

type graphDIn struct {
	B graphBOut
	C graphCOut
}

func TestGraphBuild_DiamondConstruction(t *testing.T) {
	stepA := Op[NoInput, graphAOut]("a", func(_ context.Context, _ NoInput) (graphAOut, error) {
		return graphAOut{}, nil
	})
	stepB := Op[graphBIn, graphBOut]("b", func(_ context.Context, _ graphBIn) (graphBOut, error) {
		return graphBOut{}, nil
	})
	stepC := Op[graphCIn, graphCOut]("c", func(_ context.Context, _ graphCIn) (graphCOut, error) {
		return graphCOut{}, nil
	})
	stepD := Op[graphDIn, graphDOut]("d", func(_ context.Context, _ graphDIn) (graphDOut, error) {
		return graphDOut{}, nil
	})

	job, err := NewJob("diamond_build").Add(stepA, stepB, stepC, stepD).Build()
	if err != nil {
		t.Fatalf("build job: %v", err)
	}

	edges := edgeSet(job)
	if !edges["a->b"] || !edges["a->c"] || !edges["b->d"] || !edges["c->d"] {
		t.Fatalf("unexpected inferred edges: %v", sortedEdgeKeys(edges))
	}
	assertGraphInvariants(t, job, 4, 4)
	assertTopologicalOrderValid(t, job)
}

func TestGraphEngine_LinearChain(t *testing.T) {
	job := newManualJob("linear", []manualStep{
		{key: "a"},
		{key: "b", deps: []string{"a"}},
		{key: "c", deps: []string{"b"}},
		{key: "d", deps: []string{"c"}},
	})

	order, err := topologicalOrder(job)
	if err != nil {
		t.Fatalf("topological order: %v", err)
	}
	assertStepKeysEqual(t, stepKeys(order), []string{"a", "b", "c", "d"})
	if got := graphDepth(job); got != 4 {
		t.Fatalf("expected depth 4, got %d", got)
	}
	assertRoots(t, job, []string{"a"})
	assertGraphInvariants(t, job, 4, 3)
}

func TestGraphEngine_WideFanOut(t *testing.T) {
	job := newManualJob("fanout", []manualStep{
		{key: "a"},
		{key: "b", deps: []string{"a"}},
		{key: "c", deps: []string{"a"}},
		{key: "d", deps: []string{"a"}},
		{key: "e", deps: []string{"a"}},
	})
	assertTopologicalOrderValid(t, job)
	assertGraphInvariants(t, job, 5, 4)

	children := dependentsByKey(job)["a"]
	sort.Strings(children)
	assertStepKeysEqual(t, children, []string{"b", "c", "d", "e"})
}

func TestGraphEngine_WideFanIn(t *testing.T) {
	job := newManualJob("fanin", []manualStep{
		{key: "b"},
		{key: "c"},
		{key: "d"},
		{key: "a", deps: []string{"b", "c", "d"}},
	})

	order, err := topologicalOrder(job)
	if err != nil {
		t.Fatalf("topological order: %v", err)
	}
	positions := indexByStepKey(order)
	for _, dep := range []string{"b", "c", "d"} {
		if positions[dep] >= positions["a"] {
			t.Fatalf("expected %s before a, got order %v", dep, stepKeys(order))
		}
	}
	assertGraphInvariants(t, job, 4, 3)
}

func TestGraphEngine_MultipleRoots(t *testing.T) {
	job := newManualJob("multiple_roots", []manualStep{
		{key: "a"},
		{key: "b", deps: []string{"a"}},
		{key: "c"},
		{key: "d", deps: []string{"c"}},
	})

	assertRoots(t, job, []string{"a", "c"})
	assertTopologicalOrderValid(t, job)
	assertGraphInvariants(t, job, 4, 2)
}

func TestGraphEngine_ForestInstanceIsolation(t *testing.T) {
	regA := NewRegistry()
	regB := NewRegistry()

	jobA := newManualJob("job_shared_key", []manualStep{
		{key: "a"},
		{key: "b", deps: []string{"a"}},
	})
	jobB := newManualJob("job_shared_key", []manualStep{
		{key: "x"},
		{key: "y", deps: []string{"x"}},
	})

	if err := regA.Register(jobA); err != nil {
		t.Fatalf("register regA: %v", err)
	}
	if err := regB.Register(jobB); err != nil {
		t.Fatalf("register regB: %v", err)
	}

	gotA, ok := regA.JobByKey("job_shared_key")
	if !ok {
		t.Fatalf("job missing in regA")
	}
	gotB, ok := regB.JobByKey("job_shared_key")
	if !ok {
		t.Fatalf("job missing in regB")
	}
	assertStepKeysEqual(t, sortedStepKeys(gotA), []string{"a", "b"})
	assertStepKeysEqual(t, sortedStepKeys(gotB), []string{"x", "y"})
}

func TestGraphEngine_Polytree(t *testing.T) {
	job := newManualJob("polytree", []manualStep{
		{key: "a"},
		{key: "b", deps: []string{"a"}},
		{key: "c", deps: []string{"a"}},
		{key: "d", deps: []string{"b"}},
		{key: "e", deps: []string{"c"}},
		{key: "f", deps: []string{"d"}},
	})
	assertTopologicalOrderValid(t, job)
	assertGraphInvariants(t, job, 6, 5)

	for _, step := range job.Steps {
		if len(step.DependsOn) > 1 {
			t.Fatalf("polytree node %s has %d parents", step.Key, len(step.DependsOn))
		}
	}
	for _, step := range job.Steps {
		for _, dep := range step.DependsOn {
			if hasAlternativePath(job, dep, step.Key) {
				t.Fatalf("expected no redundant edge %s->%s in polytree", dep, step.Key)
			}
		}
	}
}

func TestGraphEngine_TransitiveReduction(t *testing.T) {
	job := newManualJob("transitive", []manualStep{
		{key: "a"},
		{key: "b", deps: []string{"a"}},
		{key: "c", deps: []string{"a", "b"}}, // a->c is redundant via b
	})
	beforeReachability := reachablePairs(job)
	beforeOrder := mustTopologicalKeys(t, job)

	reduced := reduceTransitiveEdges(job)
	afterReachability := reachablePairs(reduced)
	afterOrder := mustTopologicalKeys(t, reduced)

	edges := edgeSet(reduced)
	if edges["a->c"] {
		t.Fatalf("expected transitive edge a->c removed, got edges %v", sortedEdgeKeys(edges))
	}
	assertStepKeysEqual(t, afterOrder, beforeOrder)
	if !reflect.DeepEqual(beforeReachability, afterReachability) {
		t.Fatalf("expected reachability preserved after reduction")
	}
	assertGraphInvariants(t, reduced, 3, 2)
}

func TestGraphEngine_DeepChainStress1000(t *testing.T) {
	count := 1000
	steps := make([]manualStep, 0, count)
	steps = append(steps, manualStep{key: "n0000"})
	for i := 1; i < count; i++ {
		steps = append(steps, manualStep{
			key:  nodeKey(i),
			deps: []string{nodeKey(i - 1)},
		})
	}
	job := newManualJob("deep_chain", steps)

	order, err := topologicalOrder(job)
	if err != nil {
		t.Fatalf("topological order: %v", err)
	}
	if len(order) != count {
		t.Fatalf("expected %d ordered nodes, got %d", count, len(order))
	}
	if order[0].Key != "n0000" || order[len(order)-1].Key != nodeKey(count-1) {
		t.Fatalf("unexpected endpoints: first=%s last=%s", order[0].Key, order[len(order)-1].Key)
	}
	assertGraphInvariants(t, job, count, count-1)
}

func TestGraphEngine_WideGraphStress1000(t *testing.T) {
	count := 1000
	steps := make([]manualStep, 0, count+1)
	steps = append(steps, manualStep{key: "root"})
	for i := 0; i < count; i++ {
		steps = append(steps, manualStep{
			key:  nodeKey(i),
			deps: []string{"root"},
		})
	}
	job := newManualJob("wide_graph", steps)

	order, err := topologicalOrder(job)
	if err != nil {
		t.Fatalf("topological order: %v", err)
	}
	if len(order) != count+1 {
		t.Fatalf("expected %d ordered nodes, got %d", count+1, len(order))
	}
	if order[0].Key != "root" {
		t.Fatalf("expected root first, got %s", order[0].Key)
	}
	assertGraphInvariants(t, job, count+1, count)
}

func TestGraphEngine_CycleDetection(t *testing.T) {
	job := newManualJob("cycle", []manualStep{
		{key: "a", deps: []string{"c"}},
		{key: "b", deps: []string{"a"}},
		{key: "c", deps: []string{"b"}},
	})

	order, err := topologicalOrder(job)
	if err == nil {
		t.Fatalf("expected cycle error")
	}
	if len(order) != 0 {
		t.Fatalf("expected no partial order on cycle, got %v", stepKeys(order))
	}
	if !strings.Contains(err.Error(), "cycle detected") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGraphEngine_SelfLoop(t *testing.T) {
	job := newManualJob("self_loop", []manualStep{
		{key: "a", deps: []string{"a"}},
	})
	if _, err := topologicalOrder(job); err == nil {
		t.Fatalf("expected self-loop validation failure")
	}
}

func TestGraphEngine_MissingDependencyValidation(t *testing.T) {
	job := newManualJob("missing_dep", []manualStep{
		{key: "b", deps: []string{"x"}},
	})
	err := validateJob(job)
	if err == nil {
		t.Fatalf("expected missing dependency validation error")
	}
	if !strings.Contains(err.Error(), `depends on unknown step "x"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGraphEngine_DuplicateNodeIDValidation(t *testing.T) {
	job := JobDefinition{
		Key: "duplicate_step_id",
		Steps: []StepDefinition{
			newManualStep("a"),
			newManualStep("a"),
		},
	}
	err := validateJob(job)
	if err == nil {
		t.Fatalf("expected duplicate step validation error")
	}
	if !strings.Contains(err.Error(), `duplicate step "a"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGraphEngine_TopologicalOrderDeterminism(t *testing.T) {
	job := newManualJob("determinism", []manualStep{
		{key: "b"},
		{key: "a"},
		{key: "z", deps: []string{"a", "b"}},
		{key: "y", deps: []string{"a", "b"}},
	})

	first := mustTopologicalKeys(t, job)
	for i := 0; i < 50; i++ {
		current := mustTopologicalKeys(t, job)
		assertStepKeysEqual(t, current, first)
	}
	assertTopologicalOrderValid(t, job)
}

func TestGraphEngine_MultiRootWalkValidation(t *testing.T) {
	job := newManualJob("multi_root_walk", []manualStep{
		{key: "a"},
		{key: "b", deps: []string{"a"}},
		{key: "c"},
		{key: "d", deps: []string{"c"}},
		{key: "e"},
	})
	order := mustTopologicalKeys(t, job)
	pos := indexByStepKeyFromKeys(order)

	for _, step := range job.Steps {
		for _, dep := range step.DependsOn {
			if pos[dep] >= pos[step.Key] {
				t.Fatalf("dependency order violation %s->%s in %v", dep, step.Key, order)
			}
		}
	}
	assertRoots(t, job, []string{"a", "c", "e"})
	assertGraphInvariants(t, job, 5, 2)
}

func TestGraphEngine_MultipleTopLevelInstances(t *testing.T) {
	regLeft := NewRegistry()
	regRight := NewRegistry()

	leftJob := newManualJob("same_key", []manualStep{
		{key: "a"},
		{key: "b", deps: []string{"a"}},
	})
	rightJob := newManualJob("same_key", []manualStep{
		{key: "x"},
		{key: "y", deps: []string{"x"}},
		{key: "z", deps: []string{"y"}},
	})

	if err := regLeft.Register(leftJob); err != nil {
		t.Fatalf("register left: %v", err)
	}
	if err := regRight.Register(rightJob); err != nil {
		t.Fatalf("register right: %v", err)
	}

	leftOrdered := mustTopologicalFromRegistry(t, regLeft, "same_key")
	rightOrdered := mustTopologicalFromRegistry(t, regRight, "same_key")
	assertStepKeysEqual(t, leftOrdered, []string{"a", "b"})
	assertStepKeysEqual(t, rightOrdered, []string{"x", "y", "z"})
}

type manualStep struct {
	key  string
	deps []string
}

func newManualJob(key string, steps []manualStep) JobDefinition {
	out := make([]StepDefinition, 0, len(steps))
	for _, step := range steps {
		out = append(out, newManualStep(step.key, step.deps...))
	}
	return JobDefinition{Key: key, Steps: out}
}

func newManualStep(key string, deps ...string) StepDefinition {
	copiedDeps := append([]string(nil), deps...)
	sort.Strings(copiedDeps)
	return StepDefinition{
		Key:       key,
		DependsOn: copiedDeps,
		runValue:  reflect.ValueOf(func() {}),
	}
}

func nodeKey(i int) string {
	return "n" + leftPadInt(i, 4)
}

func leftPadInt(n, width int) string {
	value := []byte{}
	if n == 0 {
		value = append(value, '0')
	} else {
		for n > 0 {
			value = append([]byte{byte('0' + (n % 10))}, value...)
			n /= 10
		}
	}
	for len(value) < width {
		value = append([]byte{'0'}, value...)
	}
	return string(value)
}

func assertGraphInvariants(t *testing.T, job JobDefinition, wantNodes int, wantEdges int) {
	t.Helper()
	if _, err := topologicalOrder(job); err != nil {
		t.Fatalf("expected acyclic graph, got %v", err)
	}
	if len(job.Steps) != wantNodes {
		t.Fatalf("expected %d nodes, got %d", wantNodes, len(job.Steps))
	}
	gotEdges := edgeCount(job)
	if gotEdges != wantEdges {
		t.Fatalf("expected %d edges, got %d", wantEdges, gotEdges)
	}
	reachable := reachableNodes(job)
	if len(reachable) != len(job.Steps) {
		t.Fatalf("expected all nodes reachable from at least one root, got %d/%d", len(reachable), len(job.Steps))
	}
}

func assertRoots(t *testing.T, job JobDefinition, want []string) {
	t.Helper()
	got := roots(job)
	sort.Strings(got)
	sort.Strings(want)
	assertStepKeysEqual(t, got, want)
}

func assertTopologicalOrderValid(t *testing.T, job JobDefinition) {
	t.Helper()
	order, err := topologicalOrder(job)
	if err != nil {
		t.Fatalf("topological order: %v", err)
	}
	if len(order) != len(job.Steps) {
		t.Fatalf("topological order length mismatch: got %d want %d", len(order), len(job.Steps))
	}
	pos := indexByStepKey(order)
	for _, step := range job.Steps {
		for _, dep := range step.DependsOn {
			if pos[dep] >= pos[step.Key] {
				t.Fatalf("dependency order violation: %s -> %s", dep, step.Key)
			}
		}
	}
}

func stepKeys(steps []StepDefinition) []string {
	out := make([]string, 0, len(steps))
	for _, step := range steps {
		out = append(out, step.Key)
	}
	return out
}

func sortedStepKeys(job JobDefinition) []string {
	keys := make([]string, 0, len(job.Steps))
	for _, step := range job.Steps {
		keys = append(keys, step.Key)
	}
	sort.Strings(keys)
	return keys
}

func indexByStepKey(steps []StepDefinition) map[string]int {
	out := make(map[string]int, len(steps))
	for idx, step := range steps {
		out[step.Key] = idx
	}
	return out
}

func indexByStepKeyFromKeys(keys []string) map[string]int {
	out := make(map[string]int, len(keys))
	for idx, key := range keys {
		out[key] = idx
	}
	return out
}

func mustTopologicalKeys(t *testing.T, job JobDefinition) []string {
	t.Helper()
	order, err := topologicalOrder(job)
	if err != nil {
		t.Fatalf("topological order: %v", err)
	}
	return stepKeys(order)
}

func mustTopologicalFromRegistry(t *testing.T, registry *Registry, key string) []string {
	t.Helper()
	job, ok := registry.JobByKey(key)
	if !ok {
		t.Fatalf("job %s not found in registry", key)
	}
	return mustTopologicalKeys(t, job)
}

func assertStepKeysEqual(t *testing.T, got []string, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length mismatch: got=%v want=%v", got, want)
	}
	for idx := range got {
		if got[idx] != want[idx] {
			t.Fatalf("mismatch at index %d: got=%v want=%v", idx, got, want)
		}
	}
}

func roots(job JobDefinition) []string {
	out := make([]string, 0, len(job.Steps))
	for _, step := range job.Steps {
		if len(step.DependsOn) == 0 {
			out = append(out, step.Key)
		}
	}
	return out
}

func edgeCount(job JobDefinition) int {
	total := 0
	for _, step := range job.Steps {
		total += len(step.DependsOn)
	}
	return total
}

func dependentsByKey(job JobDefinition) map[string][]string {
	out := make(map[string][]string)
	for _, step := range job.Steps {
		for _, dep := range step.DependsOn {
			out[dep] = append(out[dep], step.Key)
		}
	}
	return out
}

func reachableNodes(job JobDefinition) map[string]bool {
	children := dependentsByKey(job)
	seen := make(map[string]bool, len(job.Steps))
	queue := roots(job)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if seen[current] {
			continue
		}
		seen[current] = true
		for _, child := range children[current] {
			if !seen[child] {
				queue = append(queue, child)
			}
		}
	}
	return seen
}

func graphDepth(job JobDefinition) int {
	order, err := topologicalOrder(job)
	if err != nil {
		return 0
	}
	depthByKey := make(map[string]int, len(order))
	maxDepth := 0
	for _, step := range order {
		depth := 1
		for _, dep := range step.DependsOn {
			if candidate := depthByKey[dep] + 1; candidate > depth {
				depth = candidate
			}
		}
		depthByKey[step.Key] = depth
		if depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}

func edgeSet(job JobDefinition) map[string]bool {
	out := map[string]bool{}
	for _, step := range job.Steps {
		for _, dep := range step.DependsOn {
			out[dep+"->"+step.Key] = true
		}
	}
	return out
}

func sortedEdgeKeys(edges map[string]bool) []string {
	out := make([]string, 0, len(edges))
	for key := range edges {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func reachablePairs(job JobDefinition) map[string]bool {
	children := dependentsByKey(job)
	nodes := sortedStepKeys(job)
	out := make(map[string]bool)
	for _, start := range nodes {
		seen := map[string]bool{}
		queue := append([]string(nil), children[start]...)
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			if seen[current] {
				continue
			}
			seen[current] = true
			out[start+"->"+current] = true
			queue = append(queue, children[current]...)
		}
	}
	return out
}

func hasAlternativePath(job JobDefinition, from string, to string) bool {
	children := dependentsByKey(job)
	queue := append([]string(nil), children[from]...)
	seen := map[string]bool{}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current == to {
			continue
		}
		if seen[current] {
			continue
		}
		seen[current] = true
		for _, next := range children[current] {
			if next == to {
				return true
			}
			if !seen[next] {
				queue = append(queue, next)
			}
		}
	}
	return false
}

func reduceTransitiveEdges(job JobDefinition) JobDefinition {
	reduced := job
	reduced.Steps = make([]StepDefinition, 0, len(job.Steps))
	for _, step := range job.Steps {
		candidate := step
		kept := make([]string, 0, len(step.DependsOn))
		for _, dep := range step.DependsOn {
			if hasAlternativePathWithoutDirectEdge(job, dep, step.Key) {
				continue
			}
			kept = append(kept, dep)
		}
		sort.Strings(kept)
		candidate.DependsOn = kept
		reduced.Steps = append(reduced.Steps, candidate)
	}
	return reduced
}

func hasAlternativePathWithoutDirectEdge(job JobDefinition, from string, to string) bool {
	children := dependentsByKey(job)
	queue := make([]string, 0, len(children[from]))
	for _, next := range children[from] {
		if next == to {
			continue // ignore direct edge
		}
		queue = append(queue, next)
	}
	seen := map[string]bool{from: true}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current == to {
			return true
		}
		if seen[current] {
			continue
		}
		seen[current] = true
		for _, next := range children[current] {
			if !seen[next] {
				queue = append(queue, next)
			}
		}
	}
	return false
}
