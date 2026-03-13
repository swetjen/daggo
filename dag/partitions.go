package dag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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

type CustomPartitionSpec struct {
	Kind    PartitionDefinitionKind `json:"kind"`
	Version string                  `json:"version,omitempty"`
	Assets  []string                `json:"assets,omitempty"`
	Config  any                     `json:"config,omitempty"`
}

type CustomPartition interface {
	Spec() CustomPartitionSpec
	Keys(context.Context, time.Time) ([]string, error)
}

type customPartitionDefinition struct {
	provider CustomPartition
}

func (d customPartitionDefinition) Kind() PartitionDefinitionKind {
	spec := d.provider.Spec()
	kind := PartitionDefinitionKind(strings.TrimSpace(string(spec.Kind)))
	if kind == "" {
		return PartitionDefinitionDynamic
	}
	return kind
}

func (d customPartitionDefinition) PartitionKeys(ctx context.Context, now time.Time) ([]string, error) {
	keys, err := d.provider.Keys(ctx, now)
	if err != nil {
		return nil, err
	}
	return normalizeOrderedKeys(keys, "partition key")
}

func (d customPartitionDefinition) Spec() CustomPartitionSpec {
	return d.provider.Spec()
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
	TimePartitionCadenceMinutely TimePartitionCadence = "minutely"
	TimePartitionCadenceHourly   TimePartitionCadence = "hourly"
	TimePartitionCadenceDaily    TimePartitionCadence = "daily"
	TimePartitionCadenceWeekly   TimePartitionCadence = "weekly"
	TimePartitionCadenceMonthly  TimePartitionCadence = "monthly"
)

type TimeWindowPartitionDefinition struct {
	Cadence        TimePartitionCadence
	Start          time.Time
	EndOffset      int
	Location       *time.Location
	Format         string
	MinuteInterval int
}

func (d TimeWindowPartitionDefinition) Kind() PartitionDefinitionKind {
	return PartitionDefinitionTimeWindow
}

func (d TimeWindowPartitionDefinition) PartitionKeys(_ context.Context, now time.Time) ([]string, error) {
	if d.Start.IsZero() {
		return nil, fmt.Errorf("time window partition start is required")
	}
	switch d.Cadence {
	case TimePartitionCadenceMinutely, TimePartitionCadenceHourly, TimePartitionCadenceDaily, TimePartitionCadenceWeekly, TimePartitionCadenceMonthly:
	default:
		return nil, fmt.Errorf("unsupported time partition cadence %q", d.Cadence)
	}
	interval := d.interval()

	location := d.Location
	if location == nil {
		location = time.UTC
	}
	format := strings.TrimSpace(d.Format)
	if format == "" {
		format = d.defaultFormat()
	}

	start := truncateToCadence(d.Start.In(location), d.Cadence, interval)
	current := truncateToCadence(now.In(location), d.Cadence, interval)
	end := shiftByCadence(current, d.Cadence, interval, d.EndOffset)
	if start.After(end) {
		return []string{}, nil
	}

	keys := make([]string, 0)
	for cursor := start; !cursor.After(end); cursor = shiftByCadence(cursor, d.Cadence, interval, 1) {
		keys = append(keys, cursor.Format(format))
	}
	return keys, nil
}

func (d TimeWindowPartitionDefinition) interval() int {
	if d.Cadence != TimePartitionCadenceMinutely {
		return 1
	}
	if d.MinuteInterval <= 0 {
		return 1
	}
	return d.MinuteInterval
}

func (d TimeWindowPartitionDefinition) stepDuration() (time.Duration, error) {
	switch d.Cadence {
	case TimePartitionCadenceMinutely:
		return time.Duration(d.interval()) * time.Minute, nil
	case TimePartitionCadenceHourly:
		return time.Hour, nil
	case TimePartitionCadenceDaily:
		return 24 * time.Hour, nil
	case TimePartitionCadenceWeekly:
		return 7 * 24 * time.Hour, nil
	case TimePartitionCadenceMonthly:
		return 0, fmt.Errorf("monthly cadence does not map to a fixed duration")
	default:
		return 0, fmt.Errorf("unsupported time partition cadence %q", d.Cadence)
	}
}

func (d TimeWindowPartitionDefinition) defaultFormat() string {
	switch d.Cadence {
	case TimePartitionCadenceMinutely:
		return "2006-01-02T15:04"
	case TimePartitionCadenceHourly:
		return "2006-01-02T15"
	case TimePartitionCadenceWeekly:
		return "2006-01-02"
	case TimePartitionCadenceMonthly:
		return "2006-01"
	default:
		return "2006-01-02"
	}
}

func truncateToCadence(value time.Time, cadence TimePartitionCadence, interval int) time.Time {
	switch cadence {
	case TimePartitionCadenceMinutely:
		base := value.Truncate(time.Minute)
		if interval <= 1 {
			return base
		}
		minute := base.Minute()
		alignedMinute := minute - (minute % interval)
		return time.Date(base.Year(), base.Month(), base.Day(), base.Hour(), alignedMinute, 0, 0, base.Location())
	case TimePartitionCadenceHourly:
		return value.Truncate(time.Hour)
	case TimePartitionCadenceWeekly:
		year, month, day := value.Date()
		base := time.Date(year, month, day, 0, 0, 0, 0, value.Location())
		weekday := (int(base.Weekday()) + 6) % 7 // Monday=0
		return base.AddDate(0, 0, -weekday)
	case TimePartitionCadenceMonthly:
		year, month, _ := value.Date()
		return time.Date(year, month, 1, 0, 0, 0, 0, value.Location())
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

type PartitionDimension struct {
	Name       string
	Definition PartitionDefinition
}

func Dimension(name string, definition PartitionDefinition) PartitionDimension {
	return PartitionDimension{Name: name, Definition: definition}
}

type TimePartitionOption func(*TimeWindowPartitionDefinition)

func WithTimezone(name string) TimePartitionOption {
	return func(definition *TimeWindowPartitionDefinition) {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			panic("dag.WithTimezone: timezone is required")
		}
		location, err := time.LoadLocation(trimmed)
		if err != nil {
			panic(fmt.Sprintf("dag.WithTimezone: invalid timezone %q: %v", trimmed, err))
		}
		definition.Location = location
	}
}

func WithLocation(location *time.Location) TimePartitionOption {
	return func(definition *TimeWindowPartitionDefinition) {
		if location == nil {
			panic("dag.WithLocation: location is required")
		}
		definition.Location = location
	}
}

func WithEndOffset(offset int) TimePartitionOption {
	return func(definition *TimeWindowPartitionDefinition) {
		definition.EndOffset = offset
	}
}

func WithFormat(layout string) TimePartitionOption {
	return func(definition *TimeWindowPartitionDefinition) {
		trimmed := strings.TrimSpace(layout)
		if trimmed == "" {
			panic("dag.WithFormat: format is required")
		}
		definition.Format = trimmed
	}
}

func StringPartitions(keys ...string) StaticPartitionDefinition {
	definition, err := NewStaticPartitionDefinition(keys)
	if err != nil {
		panic(fmt.Sprintf("dag.StringPartitions: %v", err))
	}
	return definition
}

func MinutelyEvery(intervalMinutes int, start time.Time, opts ...TimePartitionOption) TimeWindowPartitionDefinition {
	if intervalMinutes <= 0 {
		panic("dag.MinutelyEvery: intervalMinutes must be > 0")
	}
	definition := TimeWindowPartitionDefinition{
		Cadence:        TimePartitionCadenceMinutely,
		Start:          start,
		MinuteInterval: intervalMinutes,
	}
	applyTimePartitionOptions(&definition, opts...)
	return definition
}

func HourlyFrom(start time.Time, opts ...TimePartitionOption) TimeWindowPartitionDefinition {
	definition := TimeWindowPartitionDefinition{
		Cadence: TimePartitionCadenceHourly,
		Start:   start,
	}
	applyTimePartitionOptions(&definition, opts...)
	return definition
}

func DailyFrom(start time.Time, opts ...TimePartitionOption) TimeWindowPartitionDefinition {
	definition := TimeWindowPartitionDefinition{
		Cadence: TimePartitionCadenceDaily,
		Start:   start,
	}
	applyTimePartitionOptions(&definition, opts...)
	return definition
}

func WeeklyFrom(start time.Time, opts ...TimePartitionOption) TimeWindowPartitionDefinition {
	definition := TimeWindowPartitionDefinition{
		Cadence: TimePartitionCadenceWeekly,
		Start:   start,
	}
	applyTimePartitionOptions(&definition, opts...)
	return definition
}

func MonthlyFrom(start time.Time, opts ...TimePartitionOption) TimeWindowPartitionDefinition {
	definition := TimeWindowPartitionDefinition{
		Cadence: TimePartitionCadenceMonthly,
		Start:   start,
	}
	applyTimePartitionOptions(&definition, opts...)
	return definition
}

func MultiPartitions(dimensions ...PartitionDimension) *MultiPartitionDefinition {
	if len(dimensions) != 2 {
		panic(fmt.Sprintf("dag.MultiPartitions: expected exactly 2 dimensions, got %d", len(dimensions)))
	}
	byName := make(map[string]PartitionDefinition, len(dimensions))
	for _, dimension := range dimensions {
		name := strings.TrimSpace(dimension.Name)
		if name == "" {
			panic("dag.MultiPartitions: dimension name is required")
		}
		if dimension.Definition == nil {
			panic(fmt.Sprintf("dag.MultiPartitions: definition is required for dimension %q", name))
		}
		if _, exists := byName[name]; exists {
			panic(fmt.Sprintf("dag.MultiPartitions: duplicate dimension %q", name))
		}
		byName[name] = dimension.Definition
	}
	definition, err := NewMultiPartitionDefinition(byName)
	if err != nil {
		panic(fmt.Sprintf("dag.MultiPartitions: %v", err))
	}
	return definition
}

func NewCustomPartitionDefinition(provider CustomPartition) (PartitionDefinition, error) {
	if provider == nil {
		return nil, fmt.Errorf("custom partition provider is required")
	}
	value := reflect.ValueOf(provider)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return nil, fmt.Errorf("custom partition provider is required")
	}
	spec := provider.Spec()
	if strings.TrimSpace(string(spec.Kind)) == "" {
		return nil, fmt.Errorf("custom partition kind is required")
	}
	return customPartitionDefinition{provider: provider}, nil
}

func customPartitionSpecJSON(spec CustomPartitionSpec) (string, error) {
	payload := map[string]any{
		"version": strings.TrimSpace(spec.Version),
		"config":  spec.Config,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
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

func normalizeAssetKeys(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func shiftByCadence(value time.Time, cadence TimePartitionCadence, interval int, count int) time.Time {
	if count == 0 {
		return value
	}
	switch cadence {
	case TimePartitionCadenceMinutely:
		return value.Add(time.Duration(interval*count) * time.Minute)
	case TimePartitionCadenceHourly:
		return value.Add(time.Duration(count) * time.Hour)
	case TimePartitionCadenceDaily:
		return value.AddDate(0, 0, count)
	case TimePartitionCadenceWeekly:
		return value.AddDate(0, 0, count*7)
	case TimePartitionCadenceMonthly:
		return value.AddDate(0, count, 0)
	default:
		return value.AddDate(0, 0, count)
	}
}

func applyTimePartitionOptions(definition *TimeWindowPartitionDefinition, opts ...TimePartitionOption) {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(definition)
	}
}
