package dag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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

func TestMinutelyEveryGeneratesExpectedKeys(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 1, 0, 12, 0, 0, time.UTC)

	definition := MinutelyEvery(5, start)
	keys, err := definition.PartitionKeys(context.Background(), now)
	if err != nil {
		t.Fatalf("partition keys: %v", err)
	}
	expected := []string{"2026-01-01T00:00", "2026-01-01T00:05", "2026-01-01T00:10"}
	if !slices.Equal(keys, expected) {
		t.Fatalf("expected keys %v, got %v", expected, keys)
	}
}

func TestMonthlyFromGeneratesExpectedKeys(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)

	definition := MonthlyFrom(start)
	keys, err := definition.PartitionKeys(context.Background(), now)
	if err != nil {
		t.Fatalf("partition keys: %v", err)
	}
	expected := []string{"2026-01", "2026-02", "2026-03"}
	if !slices.Equal(keys, expected) {
		t.Fatalf("expected keys %v, got %v", expected, keys)
	}
}

func TestHourlyFromGeneratesExpectedKeys(t *testing.T) {
	start := time.Date(2026, 1, 1, 10, 20, 0, 0, time.UTC)
	now := time.Date(2026, 1, 1, 12, 45, 0, 0, time.UTC)

	definition := HourlyFrom(start)
	keys, err := definition.PartitionKeys(context.Background(), now)
	if err != nil {
		t.Fatalf("partition keys: %v", err)
	}
	expected := []string{"2026-01-01T10", "2026-01-01T11", "2026-01-01T12"}
	if !slices.Equal(keys, expected) {
		t.Fatalf("expected keys %v, got %v", expected, keys)
	}
}

func TestWeeklyFromGeneratesExpectedKeys(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) // Thursday
	now := time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC) // Wednesday

	definition := WeeklyFrom(start)
	keys, err := definition.PartitionKeys(context.Background(), now)
	if err != nil {
		t.Fatalf("partition keys: %v", err)
	}
	expected := []string{"2025-12-29", "2026-01-05", "2026-01-12"}
	if !slices.Equal(keys, expected) {
		t.Fatalf("expected keys %v, got %v", expected, keys)
	}
}

func TestTimePartitionOptionsPanicsOnInvalidInputs(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	assertPanics(t, func() {
		_ = DailyFrom(start, WithTimezone(""))
	}, "dag.WithTimezone: timezone is required")

	assertPanics(t, func() {
		_ = DailyFrom(start, WithTimezone("not/a/zone"))
	}, "dag.WithTimezone: invalid timezone")

	assertPanics(t, func() {
		_ = DailyFrom(start, WithLocation(nil))
	}, "dag.WithLocation: location is required")

	assertPanics(t, func() {
		_ = DailyFrom(start, WithFormat(""))
	}, "dag.WithFormat: format is required")

	assertPanics(t, func() {
		_ = MinutelyEvery(0, start)
	}, "dag.MinutelyEvery: intervalMinutes must be > 0")
}

func TestMultiPartitionsValidation(t *testing.T) {
	assertPanics(t, func() {
		_ = MultiPartitions()
	}, "dag.MultiPartitions: expected exactly 2 dimensions")

	assertPanics(t, func() {
		_ = MultiPartitions(
			Dimension("a", StringPartitions("x")),
			Dimension("a", StringPartitions("y")),
		)
	}, "dag.MultiPartitions: duplicate dimension")

	assertPanics(t, func() {
		_ = MultiPartitions(
			Dimension("a", nil),
			Dimension("b", StringPartitions("x")),
		)
	}, "dag.MultiPartitions: definition is required")

	definition := MultiPartitions(
		Dimension("date", DailyFrom(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), WithEndOffset(-1))),
		Dimension("region", StringPartitions("us", "eu")),
	)
	keys, err := definition.PartitionKeys(context.Background(), time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("partition keys: %v", err)
	}
	if len(keys) == 0 {
		t.Fatalf("expected multi partition keys")
	}
}

type customPartitionTestProvider struct {
	spec CustomPartitionSpec
	keys []string
	err  error
}

func (p customPartitionTestProvider) Spec() CustomPartitionSpec {
	return p.spec
}

func (p customPartitionTestProvider) Keys(context.Context, time.Time) ([]string, error) {
	if p.err != nil {
		return nil, p.err
	}
	return slices.Clone(p.keys), nil
}

type customPartitionNilPtrProvider struct{}

func (*customPartitionNilPtrProvider) Spec() CustomPartitionSpec {
	return CustomPartitionSpec{Kind: PartitionDefinitionStatic}
}

func (*customPartitionNilPtrProvider) Keys(context.Context, time.Time) ([]string, error) {
	return []string{"k"}, nil
}

func TestNewCustomPartitionDefinitionValidation(t *testing.T) {
	_, err := NewCustomPartitionDefinition(nil)
	if err == nil || !strings.Contains(err.Error(), "custom partition provider is required") {
		t.Fatalf("expected nil provider error, got %v", err)
	}

	var nilPtr *customPartitionNilPtrProvider
	_, err = NewCustomPartitionDefinition(nilPtr)
	if err == nil || !strings.Contains(err.Error(), "custom partition provider is required") {
		t.Fatalf("expected typed nil provider error, got %v", err)
	}

	_, err = NewCustomPartitionDefinition(customPartitionTestProvider{
		spec: CustomPartitionSpec{},
		keys: []string{"a"},
	})
	if err == nil || !strings.Contains(err.Error(), "custom partition kind is required") {
		t.Fatalf("expected missing kind error, got %v", err)
	}
}

func TestCustomPartitionDefinitionPartitionKeysNormalization(t *testing.T) {
	definition, err := NewCustomPartitionDefinition(customPartitionTestProvider{
		spec: CustomPartitionSpec{Kind: PartitionDefinitionStatic},
		keys: []string{"a", "b"},
	})
	if err != nil {
		t.Fatalf("new custom partition definition: %v", err)
	}
	keys, err := definition.PartitionKeys(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("partition keys: %v", err)
	}
	if !slices.Equal(keys, []string{"a", "b"}) {
		t.Fatalf("expected keys [a b], got %v", keys)
	}

	definition, err = NewCustomPartitionDefinition(customPartitionTestProvider{
		spec: CustomPartitionSpec{Kind: PartitionDefinitionStatic},
		keys: []string{"a", "a"},
	})
	if err != nil {
		t.Fatalf("new custom partition definition: %v", err)
	}
	_, err = definition.PartitionKeys(context.Background(), time.Now().UTC())
	if err == nil || !strings.Contains(err.Error(), "duplicate partition key") {
		t.Fatalf("expected duplicate key error, got %v", err)
	}
}

func TestCustomPartitionSpecJSON(t *testing.T) {
	encoded, err := customPartitionSpecJSON(CustomPartitionSpec{
		Kind:    PartitionDefinitionStatic,
		Version: "v2",
		Config: map[string]any{
			"window": "daily",
		},
	})
	if err != nil {
		t.Fatalf("encode custom partition spec: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(encoded), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["version"] != "v2" {
		t.Fatalf("expected version v2, got %#v", payload["version"])
	}
	if _, ok := payload["config"].(map[string]any); !ok {
		t.Fatalf("expected config object, got %#v", payload["config"])
	}
}

type unsupportedPartitionDefinition struct{}

func (unsupportedPartitionDefinition) Kind() PartitionDefinitionKind {
	return PartitionDefinitionStatic
}

func (unsupportedPartitionDefinition) PartitionKeys(context.Context, time.Time) ([]string, error) {
	return []string{"x"}, nil
}

func TestSerializePartitionDefinitionCoverage(t *testing.T) {
	now := time.Now().UTC()

	_, _, err := serializePartitionDefinition(nil)
	if err == nil {
		t.Fatalf("expected nil definition error")
	}

	staticDef := StringPartitions("a", "b")
	kind, payload, err := serializePartitionDefinition(staticDef)
	if err != nil {
		t.Fatalf("serialize static: %v", err)
	}
	if kind != PartitionDefinitionStatic || !strings.Contains(payload, "\"keys\"") {
		t.Fatalf("unexpected static serialization: kind=%q payload=%s", kind, payload)
	}

	dynamicDef := DynamicPartitionDefinition{Name: "regions", Store: dynamicStoreStub{keysByName: map[string][]string{"regions": {"us"}}}}
	kind, payload, err = serializePartitionDefinition(dynamicDef)
	if err != nil {
		t.Fatalf("serialize dynamic: %v", err)
	}
	if kind != PartitionDefinitionDynamic || !strings.Contains(payload, "\"name\"") {
		t.Fatalf("unexpected dynamic serialization: kind=%q payload=%s", kind, payload)
	}

	timeDef := DailyFrom(now, WithTimezone("UTC"), WithFormat("2006-01-02"), WithEndOffset(1))
	kind, payload, err = serializePartitionDefinition(timeDef)
	if err != nil {
		t.Fatalf("serialize time window: %v", err)
	}
	if kind != PartitionDefinitionTimeWindow || !strings.Contains(payload, "\"cadence\"") {
		t.Fatalf("unexpected time serialization: kind=%q payload=%s", kind, payload)
	}

	multiDef := MultiPartitions(
		Dimension("date", DailyFrom(now, WithEndOffset(-1))),
		Dimension("region", StringPartitions("us", "eu")),
	)
	kind, payload, err = serializePartitionDefinition(multiDef)
	if err != nil {
		t.Fatalf("serialize multi: %v", err)
	}
	if kind != PartitionDefinitionMulti || !strings.Contains(payload, "\"dimensions\"") {
		t.Fatalf("unexpected multi serialization: kind=%q payload=%s", kind, payload)
	}

	customDef, err := NewCustomPartitionDefinition(customPartitionTestProvider{
		spec: CustomPartitionSpec{
			Kind:    PartitionDefinitionStatic,
			Version: "v1",
			Config:  map[string]any{"scope": "custom"},
		},
		keys: []string{"x"},
	})
	if err != nil {
		t.Fatalf("build custom definition: %v", err)
	}
	kind, payload, err = serializePartitionDefinition(customDef)
	if err != nil {
		t.Fatalf("serialize custom: %v", err)
	}
	if kind != PartitionDefinitionStatic || !strings.Contains(payload, "\"scope\"") {
		t.Fatalf("unexpected custom serialization: kind=%q payload=%s", kind, payload)
	}

	_, _, err = serializePartitionDefinition(unsupportedPartitionDefinition{})
	if err == nil || !strings.Contains(err.Error(), "unsupported partition definition type") {
		t.Fatalf("expected unsupported type error, got %v", err)
	}
}

func assertPanics(t *testing.T, fn func(), contains string) {
	t.Helper()
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("expected panic containing %q", contains)
		}
		message := fmt.Sprint(recovered)
		if !strings.Contains(message, contains) {
			t.Fatalf("expected panic containing %q, got %q", contains, message)
		}
	}()
	fn()
}
