package dag

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type testOutput struct {
	Name string `json:"name"`
}

type testAltOutput struct {
	Value int `json:"value"`
}

type testSingularInput struct {
	In testOutput `json:"in"`
}

type testSliceInput struct {
	Sources []testOutput `json:"sources"`
}

type testPointerInput struct {
	Maybe *testOutput `json:"maybe"`
}

type testTransformInput struct {
	Prev testOutput `json:"prev"`
}

func TestBuildSingularInputMissingProvider(t *testing.T) {
	consumer := Define[testSingularInput, testAltOutput]("consumer", func(_ context.Context, _ testSingularInput) (testAltOutput, error) {
		return testAltOutput{}, nil
	})

	_, err := NewJob("missing_singular").Add(consumer).Build()
	if err == nil {
		t.Fatalf("expected build error")
	}
	if !strings.Contains(err.Error(), "requires exactly 1 provider, found 0") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildSingularInputAmbiguousProvider(t *testing.T) {
	producerA := Define[NoInput, testOutput]("producer_a", func(_ context.Context, _ NoInput) (testOutput, error) {
		return testOutput{Name: "a"}, nil
	})
	producerB := Define[NoInput, testOutput]("producer_b", func(_ context.Context, _ NoInput) (testOutput, error) {
		return testOutput{Name: "b"}, nil
	})
	consumer := Define[testSingularInput, testAltOutput]("consumer", func(_ context.Context, _ testSingularInput) (testAltOutput, error) {
		return testAltOutput{}, nil
	})

	_, err := NewJob("ambiguous_singular").Add(producerA, producerB, consumer).Build()
	if err == nil {
		t.Fatalf("expected build error")
	}
	message := err.Error()
	if !strings.Contains(message, "ambiguous") || !strings.Contains(message, "producer_a") || !strings.Contains(message, "producer_b") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildPointerInputOptionalNil(t *testing.T) {
	consumer := Define[testPointerInput, testAltOutput]("consumer", func(_ context.Context, in testPointerInput) (testAltOutput, error) {
		if in.Maybe != nil {
			return testAltOutput{}, ErrContract("expected nil pointer")
		}
		return testAltOutput{Value: 1}, nil
	})

	job, err := NewJob("pointer_optional").Add(consumer).Build()
	if err != nil {
		t.Fatalf("build job: %v", err)
	}

	step := mustFindStep(t, job, "consumer")
	binding := mustFindBinding(t, step, "Maybe")
	if binding.Mode != InputResolutionPointer {
		t.Fatalf("expected pointer mode, got %q", binding.Mode)
	}
	if len(binding.Providers) != 0 {
		t.Fatalf("expected zero providers, got %v", binding.Providers)
	}

	input, err := hydrateTypedInput(step, map[string]json.RawMessage{})
	if err != nil {
		t.Fatalf("hydrate pointer input: %v", err)
	}
	decoded := input.Interface().(testPointerInput)
	if decoded.Maybe != nil {
		t.Fatalf("expected nil pointer input")
	}
}

func TestBuildPointerInputAmbiguousProvider(t *testing.T) {
	producerA := Define[NoInput, testOutput]("producer_a", func(_ context.Context, _ NoInput) (testOutput, error) {
		return testOutput{Name: "a"}, nil
	})
	producerB := Define[NoInput, testOutput]("producer_b", func(_ context.Context, _ NoInput) (testOutput, error) {
		return testOutput{Name: "b"}, nil
	})
	consumer := Define[testPointerInput, testAltOutput]("consumer", func(_ context.Context, _ testPointerInput) (testAltOutput, error) {
		return testAltOutput{}, nil
	})

	_, err := NewJob("ambiguous_pointer").Add(producerA, producerB, consumer).Build()
	if err == nil {
		t.Fatalf("expected build error")
	}
	if !strings.Contains(err.Error(), "pointer input allows 0 or 1 provider") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildPointerInputSingleProvider(t *testing.T) {
	producer := Define[NoInput, testOutput]("producer", func(_ context.Context, _ NoInput) (testOutput, error) {
		return testOutput{Name: "single"}, nil
	})
	consumer := Define[testPointerInput, testAltOutput]("consumer", func(_ context.Context, in testPointerInput) (testAltOutput, error) {
		if in.Maybe == nil {
			return testAltOutput{}, ErrContract("expected non-nil pointer")
		}
		return testAltOutput{Value: len(in.Maybe.Name)}, nil
	})

	job, err := NewJob("pointer_single").Add(producer, consumer).Build()
	if err != nil {
		t.Fatalf("build job: %v", err)
	}

	step := mustFindStep(t, job, "consumer")
	binding := mustFindBinding(t, step, "Maybe")
	if binding.Mode != InputResolutionPointer {
		t.Fatalf("expected pointer mode, got %q", binding.Mode)
	}
	if len(binding.Providers) != 1 || binding.Providers[0] != "producer" {
		t.Fatalf("expected provider [producer], got %v", binding.Providers)
	}

	outputs := map[string]json.RawMessage{}
	payload, _ := json.Marshal(testOutput{Name: "hydrated"})
	outputs["producer"] = payload

	input, err := hydrateTypedInput(step, outputs)
	if err != nil {
		t.Fatalf("hydrate pointer input: %v", err)
	}
	decoded := input.Interface().(testPointerInput)
	if decoded.Maybe == nil || decoded.Maybe.Name != "hydrated" {
		t.Fatalf("expected hydrated pointer output, got %+v", decoded.Maybe)
	}
}

func TestBuildSliceInputFanInOrderIsTopological(t *testing.T) {
	producerRoot := Define[NoInput, testOutput]("z_source", func(_ context.Context, _ NoInput) (testOutput, error) {
		return testOutput{Name: "z"}, nil
	})
	producerDerived := Define[testTransformInput, testOutput]("a_source", func(_ context.Context, in testTransformInput) (testOutput, error) {
		return testOutput{Name: in.Prev.Name + "_a"}, nil
	})
	merger := Define[testSliceInput, testAltOutput]("merge", func(_ context.Context, in testSliceInput) (testAltOutput, error) {
		return testAltOutput{Value: len(in.Sources)}, nil
	})

	job, err := NewJob("slice_fanin").Add(producerDerived, producerRoot, merger).Build()
	if err != nil {
		t.Fatalf("build job: %v", err)
	}

	mergeStep := mustFindStep(t, job, "merge")
	binding := mustFindBinding(t, mergeStep, "Sources")
	if binding.Mode != InputResolutionSlice {
		t.Fatalf("expected slice mode, got %q", binding.Mode)
	}
	if len(binding.Providers) != 2 {
		t.Fatalf("expected two providers, got %v", binding.Providers)
	}
	if binding.Providers[0] != "z_source" || binding.Providers[1] != "a_source" {
		t.Fatalf("expected topological provider order [z_source a_source], got %v", binding.Providers)
	}

	outputs := map[string]json.RawMessage{}
	rootPayload, _ := json.Marshal(testOutput{Name: "root"})
	derivedPayload, _ := json.Marshal(testOutput{Name: "derived"})
	outputs["z_source"] = rootPayload
	outputs["a_source"] = derivedPayload

	input, err := hydrateTypedInput(mergeStep, outputs)
	if err != nil {
		t.Fatalf("hydrate slice input: %v", err)
	}
	decoded := input.Interface().(testSliceInput)
	if len(decoded.Sources) != 2 {
		t.Fatalf("expected two sources, got %d", len(decoded.Sources))
	}
	if decoded.Sources[0].Name != "root" || decoded.Sources[1].Name != "derived" {
		t.Fatalf("expected hydrated order [root derived], got [%s %s]", decoded.Sources[0].Name, decoded.Sources[1].Name)
	}
}

func TestBuildSliceInputAllowsZeroProviders(t *testing.T) {
	consumer := Define[testSliceInput, testAltOutput]("consumer", func(_ context.Context, in testSliceInput) (testAltOutput, error) {
		return testAltOutput{Value: len(in.Sources)}, nil
	})

	job, err := NewJob("slice_zero").Add(consumer).Build()
	if err != nil {
		t.Fatalf("build job: %v", err)
	}

	step := mustFindStep(t, job, "consumer")
	binding := mustFindBinding(t, step, "Sources")
	if binding.Mode != InputResolutionSlice {
		t.Fatalf("expected slice mode, got %q", binding.Mode)
	}
	if len(binding.Providers) != 0 {
		t.Fatalf("expected zero providers, got %v", binding.Providers)
	}

	input, err := hydrateTypedInput(step, map[string]json.RawMessage{})
	if err != nil {
		t.Fatalf("hydrate slice input: %v", err)
	}
	decoded := input.Interface().(testSliceInput)
	if len(decoded.Sources) != 0 {
		t.Fatalf("expected empty slice, got %d", len(decoded.Sources))
	}
}

func mustFindStep(t *testing.T, job JobDefinition, key string) StepDefinition {
	t.Helper()
	for _, step := range job.Steps {
		if step.Key == key {
			return step
		}
	}
	t.Fatalf("step %s not found", key)
	return StepDefinition{}
}

func mustFindBinding(t *testing.T, step StepDefinition, fieldName string) InputBinding {
	t.Helper()
	for _, binding := range step.Bindings {
		if binding.FieldName == fieldName {
			return binding
		}
	}
	t.Fatalf("binding for field %s not found", fieldName)
	return InputBinding{}
}
