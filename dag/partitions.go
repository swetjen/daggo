package dag

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

var ErrDynamicPartitionStoreRequired = errors.New("dynamic partition store is required")

type PartitionDefinitionKind string

const (
	PartitionDefinitionStatic     PartitionDefinitionKind = "static"
	PartitionDefinitionTimeWindow PartitionDefinitionKind = "time_window"
	PartitionDefinitionDynamic    PartitionDefinitionKind = "dynamic"
	PartitionDefinitionMulti      PartitionDefinitionKind = "multi"
)

type PartitionDefinition interface {
	Kind() PartitionDefinitionKind
	PartitionKeys(context.Context, time.Time) ([]string, error)
}

type StaticPartitionDefinition struct {
	keys []string
}

func NewStaticPartitionDefinition(keys []string) (StaticPartitionDefinition, error) {
	normalized, err := normalizeOrderedKeys(keys, "partition key")
	if err != nil {
		return StaticPartitionDefinition{}, err
	}
	return StaticPartitionDefinition{keys: normalized}, nil
}

func (d StaticPartitionDefinition) Kind() PartitionDefinitionKind {
	return PartitionDefinitionStatic
}

func (d StaticPartitionDefinition) PartitionKeys(_ context.Context, _ time.Time) ([]string, error) {
	return cloneStrings(d.keys), nil
}

type DynamicPartitionStore interface {
	DynamicPartitionKeys(context.Context, string) ([]string, error)
}

type DynamicPartitionDefinition struct {
	Name  string
	Store DynamicPartitionStore
}

func (d DynamicPartitionDefinition) Kind() PartitionDefinitionKind {
	return PartitionDefinitionDynamic
}

func (d DynamicPartitionDefinition) PartitionKeys(ctx context.Context, _ time.Time) ([]string, error) {
	name := strings.TrimSpace(d.Name)
	if name == "" {
		return nil, fmt.Errorf("dynamic partition definition name is required")
	}
	if d.Store == nil {
		return nil, ErrDynamicPartitionStoreRequired
	}
	keys, err := d.Store.DynamicPartitionKeys(ctx, name)
	if err != nil {
		return nil, err
	}
	return normalizeOrderedKeys(keys, "partition key")
}

type TimePartitionCadence string

const (
	TimePartitionCadenceDaily  TimePartitionCadence = "daily"
	TimePartitionCadenceHourly TimePartitionCadence = "hourly"
)

type TimeWindowPartitionDefinition struct {
	Cadence   TimePartitionCadence
	Start     time.Time
	EndOffset int
	Location  *time.Location
	Format    string
}

func (d TimeWindowPartitionDefinition) Kind() PartitionDefinitionKind {
	return PartitionDefinitionTimeWindow
}

func (d TimeWindowPartitionDefinition) PartitionKeys(_ context.Context, now time.Time) ([]string, error) {
	if d.Start.IsZero() {
		return nil, fmt.Errorf("time window partition start is required")
	}
	step, err := d.stepDuration()
	if err != nil {
		return nil, err
	}

	location := d.Location
	if location == nil {
		location = time.UTC
	}
	format := strings.TrimSpace(d.Format)
	if format == "" {
		format = d.defaultFormat()
	}

	start := truncateToCadence(d.Start.In(location), d.Cadence)
	current := truncateToCadence(now.In(location), d.Cadence)
	end := current.Add(time.Duration(d.EndOffset) * step)
	if start.After(end) {
		return []string{}, nil
	}

	keys := make([]string, 0)
	for cursor := start; !cursor.After(end); cursor = cursor.Add(step) {
		keys = append(keys, cursor.Format(format))
	}
	return keys, nil
}

func (d TimeWindowPartitionDefinition) stepDuration() (time.Duration, error) {
	switch d.Cadence {
	case TimePartitionCadenceDaily:
		return 24 * time.Hour, nil
	case TimePartitionCadenceHourly:
		return time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported time partition cadence %q", d.Cadence)
	}
}

func (d TimeWindowPartitionDefinition) defaultFormat() string {
	switch d.Cadence {
	case TimePartitionCadenceHourly:
		return "2006-01-02T15"
	default:
		return "2006-01-02"
	}
}

func truncateToCadence(value time.Time, cadence TimePartitionCadence) time.Time {
	switch cadence {
	case TimePartitionCadenceHourly:
		return value.Truncate(time.Hour)
	default:
		year, month, day := value.Date()
		return time.Date(year, month, day, 0, 0, 0, 0, value.Location())
	}
}

type MultiPartitionDefinition struct {
	dimensionNames []string
	dimensions     map[string]PartitionDefinition
}

func NewMultiPartitionDefinition(dimensions map[string]PartitionDefinition) (*MultiPartitionDefinition, error) {
	if len(dimensions) != 2 {
		return nil, fmt.Errorf("multi partition definition requires exactly 2 dimensions, got %d", len(dimensions))
	}
	names := make([]string, 0, len(dimensions))
	normalizedDimensions := make(map[string]PartitionDefinition, len(dimensions))
	for rawName, definition := range dimensions {
		name := strings.TrimSpace(rawName)
		if name == "" {
			return nil, fmt.Errorf("multi partition dimension name is required")
		}
		if definition == nil {
			return nil, fmt.Errorf("multi partition dimension %q definition is required", name)
		}
		if _, exists := normalizedDimensions[name]; exists {
			return nil, fmt.Errorf("duplicate multi partition dimension %q", name)
		}
		names = append(names, name)
		normalizedDimensions[name] = definition
	}
	sort.Strings(names)
	return &MultiPartitionDefinition{
		dimensionNames: names,
		dimensions:     normalizedDimensions,
	}, nil
}

func (d *MultiPartitionDefinition) Kind() PartitionDefinitionKind {
	return PartitionDefinitionMulti
}

func (d *MultiPartitionDefinition) DimensionNames() []string {
	if d == nil {
		return nil
	}
	return cloneStrings(d.dimensionNames)
}

func (d *MultiPartitionDefinition) PartitionKeys(ctx context.Context, now time.Time) ([]string, error) {
	if d == nil {
		return nil, fmt.Errorf("multi partition definition is nil")
	}
	if len(d.dimensionNames) != 2 {
		return nil, fmt.Errorf("multi partition definition requires exactly 2 dimensions, got %d", len(d.dimensionNames))
	}

	firstName := d.dimensionNames[0]
	secondName := d.dimensionNames[1]

	firstKeys, err := d.dimensions[firstName].PartitionKeys(ctx, now)
	if err != nil {
		return nil, err
	}
	secondKeys, err := d.dimensions[secondName].PartitionKeys(ctx, now)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(firstKeys)*len(secondKeys))
	for _, firstKey := range firstKeys {
		for _, secondKey := range secondKeys {
			out = append(out, firstKey+"|"+secondKey)
		}
	}
	return out, nil
}

type PartitionSelectionClauseKind string

const (
	PartitionSelectionClauseSingle PartitionSelectionClauseKind = "single"
	PartitionSelectionClauseRange  PartitionSelectionClauseKind = "range"
	PartitionSelectionClauseSubset PartitionSelectionClauseKind = "subset"
)

type PartitionSelectionClause struct {
	Kind       PartitionSelectionClauseKind
	Key        string
	RangeStart string
	RangeEnd   string
	Keys       []string
}

type PartitionSelection struct {
	Clauses []PartitionSelectionClause
}

func SelectPartitionKey(key string) PartitionSelection {
	return PartitionSelection{
		Clauses: []PartitionSelectionClause{
			{
				Kind: PartitionSelectionClauseSingle,
				Key:  key,
			},
		},
	}
}

func SelectPartitionRange(start, end string) PartitionSelection {
	return PartitionSelection{
		Clauses: []PartitionSelectionClause{
			{
				Kind:       PartitionSelectionClauseRange,
				RangeStart: start,
				RangeEnd:   end,
			},
		},
	}
}

func SelectPartitionSubset(keys ...string) PartitionSelection {
	return PartitionSelection{
		Clauses: []PartitionSelectionClause{
			{
				Kind: PartitionSelectionClauseSubset,
				Keys: cloneStrings(keys),
			},
		},
	}
}

func MergePartitionSelections(selections ...PartitionSelection) PartitionSelection {
	merged := PartitionSelection{Clauses: []PartitionSelectionClause{}}
	for _, selection := range selections {
		merged.Clauses = append(merged.Clauses, selection.Clauses...)
	}
	return merged
}

type PartitionSelectionMode string

const (
	PartitionSelectionModeSingle PartitionSelectionMode = "single"
	PartitionSelectionModeRange  PartitionSelectionMode = "range"
	PartitionSelectionModeSubset PartitionSelectionMode = "subset"
)

type PartitionSelectionRange struct {
	StartKey string
	EndKey   string
}

type NormalizedPartitionSelection struct {
	Mode   PartitionSelectionMode
	Keys   []string
	Ranges []PartitionSelectionRange
}

func NormalizePartitionSelectionForDefinition(
	ctx context.Context,
	definition PartitionDefinition,
	selection PartitionSelection,
	now time.Time,
) (NormalizedPartitionSelection, error) {
	if definition == nil {
		return NormalizedPartitionSelection{}, fmt.Errorf("partition definition is required")
	}
	keys, err := definition.PartitionKeys(ctx, now)
	if err != nil {
		return NormalizedPartitionSelection{}, err
	}
	return NormalizePartitionSelection(keys, selection)
}

func NormalizePartitionSelection(allKeys []string, selection PartitionSelection) (NormalizedPartitionSelection, error) {
	orderedKeys, err := normalizeOrderedKeys(allKeys, "partition key")
	if err != nil {
		return NormalizedPartitionSelection{}, err
	}
	if len(selection.Clauses) == 0 {
		return NormalizedPartitionSelection{}, fmt.Errorf("partition selection requires at least one clause")
	}

	indexByKey := make(map[string]int, len(orderedKeys))
	for index, key := range orderedKeys {
		indexByKey[key] = index
	}
	selectedIndexes := make([]bool, len(orderedKeys))
	for _, clause := range selection.Clauses {
		switch clause.Kind {
		case PartitionSelectionClauseSingle:
			key := strings.TrimSpace(clause.Key)
			if key == "" {
				return NormalizedPartitionSelection{}, fmt.Errorf("partition selection single clause requires key")
			}
			index, ok := indexByKey[key]
			if !ok {
				return NormalizedPartitionSelection{}, fmt.Errorf("partition selection key %q not found", key)
			}
			selectedIndexes[index] = true
		case PartitionSelectionClauseRange:
			startKey := strings.TrimSpace(clause.RangeStart)
			endKey := strings.TrimSpace(clause.RangeEnd)
			if startKey == "" || endKey == "" {
				return NormalizedPartitionSelection{}, fmt.Errorf("partition selection range clause requires start and end")
			}
			startIndex, ok := indexByKey[startKey]
			if !ok {
				return NormalizedPartitionSelection{}, fmt.Errorf("partition selection key %q not found", startKey)
			}
			endIndex, ok := indexByKey[endKey]
			if !ok {
				return NormalizedPartitionSelection{}, fmt.Errorf("partition selection key %q not found", endKey)
			}
			if startIndex > endIndex {
				startIndex, endIndex = endIndex, startIndex
			}
			for index := startIndex; index <= endIndex; index++ {
				selectedIndexes[index] = true
			}
		case PartitionSelectionClauseSubset:
			if len(clause.Keys) == 0 {
				return NormalizedPartitionSelection{}, fmt.Errorf("partition selection subset clause requires at least one key")
			}
			for _, rawKey := range clause.Keys {
				key := strings.TrimSpace(rawKey)
				if key == "" {
					return NormalizedPartitionSelection{}, fmt.Errorf("partition selection subset clause contains empty key")
				}
				index, ok := indexByKey[key]
				if !ok {
					return NormalizedPartitionSelection{}, fmt.Errorf("partition selection key %q not found", key)
				}
				selectedIndexes[index] = true
			}
		default:
			return NormalizedPartitionSelection{}, fmt.Errorf("unsupported partition selection clause kind %q", clause.Kind)
		}
	}

	selectedKeys := make([]string, 0)
	for index, selected := range selectedIndexes {
		if selected {
			selectedKeys = append(selectedKeys, orderedKeys[index])
		}
	}
	if len(selectedKeys) == 0 {
		return NormalizedPartitionSelection{}, fmt.Errorf("partition selection resolved to an empty key set")
	}

	ranges := make([]PartitionSelectionRange, 0)
	segmentStart := -1
	for index, selected := range selectedIndexes {
		if selected && segmentStart == -1 {
			segmentStart = index
		}
		lastIndex := index == len(selectedIndexes)-1
		if segmentStart == -1 {
			continue
		}
		if !selected || lastIndex {
			segmentEnd := index - 1
			if selected && lastIndex {
				segmentEnd = index
			}
			ranges = append(ranges, PartitionSelectionRange{
				StartKey: orderedKeys[segmentStart],
				EndKey:   orderedKeys[segmentEnd],
			})
			segmentStart = -1
		}
	}

	mode := PartitionSelectionModeSubset
	if len(selectedKeys) == 1 {
		mode = PartitionSelectionModeSingle
	} else if len(ranges) == 1 {
		mode = PartitionSelectionModeRange
	}

	return NormalizedPartitionSelection{
		Mode:   mode,
		Keys:   selectedKeys,
		Ranges: ranges,
	}, nil
}

func normalizeOrderedKeys(keys []string, label string) ([]string, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("%s list is empty", label)
	}
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, rawKey := range keys {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			return nil, fmt.Errorf("%s is empty", label)
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate %s %q", label, key)
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out, nil
}

func cloneStrings(values []string) []string {
	out := make([]string, len(values))
	copy(out, values)
	return out
}
