package dag

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

type dynamicStoreStub struct {
	keysByName map[string][]string
}

func (s dynamicStoreStub) DynamicPartitionKeys(_ context.Context, name string) ([]string, error) {
	return slices.Clone(s.keysByName[name]), nil
}

func TestStaticPartitionDefinitionPartitionKeys(t *testing.T) {
	definition, err := NewStaticPartitionDefinition([]string{"us", "eu"})
	if err != nil {
		t.Fatalf("create static partition definition: %v", err)
	}

	keys, err := definition.PartitionKeys(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("partition keys: %v", err)
	}
	expected := []string{"us", "eu"}
	if !slices.Equal(keys, expected) {
		t.Fatalf("expected keys %v, got %v", expected, keys)
	}
}

func TestDynamicPartitionDefinitionRequiresStore(t *testing.T) {
	definition := DynamicPartitionDefinition{Name: "regions"}
	_, err := definition.PartitionKeys(context.Background(), time.Now().UTC())
	if !errors.Is(err, ErrDynamicPartitionStoreRequired) {
		t.Fatalf("expected ErrDynamicPartitionStoreRequired, got %v", err)
	}
}

func TestDynamicPartitionDefinitionPartitionKeys(t *testing.T) {
	definition := DynamicPartitionDefinition{
		Name: "regions",
		Store: dynamicStoreStub{
			keysByName: map[string][]string{
				"regions": {"us", "eu"},
			},
		},
	}

	keys, err := definition.PartitionKeys(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("partition keys: %v", err)
	}
	expected := []string{"us", "eu"}
	if !slices.Equal(keys, expected) {
		t.Fatalf("expected keys %v, got %v", expected, keys)
	}
}

func TestNewMultiPartitionDefinitionRequiresExactlyTwoDimensions(t *testing.T) {
	staticOne, err := NewStaticPartitionDefinition([]string{"a"})
	if err != nil {
		t.Fatalf("create static definition: %v", err)
	}
	staticTwo, err := NewStaticPartitionDefinition([]string{"b"})
	if err != nil {
		t.Fatalf("create static definition: %v", err)
	}
	staticThree, err := NewStaticPartitionDefinition([]string{"c"})
	if err != nil {
		t.Fatalf("create static definition: %v", err)
	}

	_, err = NewMultiPartitionDefinition(map[string]PartitionDefinition{
		"first":  staticOne,
		"second": staticTwo,
		"third":  staticThree,
	})
	if err == nil {
		t.Fatalf("expected error for >2 dimensions")
	}
}

func TestNormalizePartitionSelectionRange(t *testing.T) {
	normalized, err := NormalizePartitionSelection(
		[]string{"a", "b", "c", "d"},
		SelectPartitionRange("a", "c"),
	)
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}

	expectedKeys := []string{"a", "b", "c"}
	if !slices.Equal(normalized.Keys, expectedKeys) {
		t.Fatalf("expected keys %v, got %v", expectedKeys, normalized.Keys)
	}
	if normalized.Mode != PartitionSelectionModeRange {
		t.Fatalf("expected mode %q, got %q", PartitionSelectionModeRange, normalized.Mode)
	}
}

func TestNormalizePartitionSelectionUnknownRangeEndpoint(t *testing.T) {
	_, err := NormalizePartitionSelection(
		[]string{"a", "b", "c", "d"},
		SelectPartitionRange("a", "z"),
	)
	if err == nil {
		t.Fatalf("expected error for unknown range endpoint")
	}
}

func TestNormalizePartitionSelectionSubsetRejectsUnknownKey(t *testing.T) {
	_, err := NormalizePartitionSelection(
		[]string{"a", "b", "c", "d"},
		SelectPartitionSubset("a", "z"),
	)
	if err == nil {
		t.Fatalf("expected error for unknown subset key")
	}
}

func TestNormalizePartitionSelectionNonContiguousClauses(t *testing.T) {
	selection := MergePartitionSelections(
		SelectPartitionKey("a"),
		SelectPartitionKey("c"),
	)
	normalized, err := NormalizePartitionSelection(
		[]string{"a", "b", "c", "d"},
		selection,
	)
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}
	expected := []string{"a", "c"}
	if !slices.Equal(normalized.Keys, expected) {
		t.Fatalf("expected keys %v, got %v", expected, normalized.Keys)
	}
	if normalized.Mode != PartitionSelectionModeSubset {
		t.Fatalf("expected mode %q, got %q", PartitionSelectionModeSubset, normalized.Mode)
	}
}

func TestNormalizePartitionSelectionForDefinition(t *testing.T) {
	definition, err := NewStaticPartitionDefinition([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("create static partition definition: %v", err)
	}
	normalized, err := NormalizePartitionSelectionForDefinition(
		context.Background(),
		definition,
		SelectPartitionSubset("a", "c"),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("normalize for definition: %v", err)
	}
	expected := []string{"a", "c"}
	if !slices.Equal(normalized.Keys, expected) {
		t.Fatalf("expected keys %v, got %v", expected, normalized.Keys)
	}
}
