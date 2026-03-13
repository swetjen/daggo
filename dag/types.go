package dag

import (
	"context"
	"reflect"
)

type RunMeta struct {
	RunID     int64
	JobKey    string
	StepKey   string
	Attempt   int64
	Params    map[string]any
	Partition *RunPartitionMeta
}

type RunPartitionMeta struct {
	DefinitionID  int64
	SelectionMode PartitionSelectionMode
	Keys          []string
	Ranges        []PartitionSelectionRange
	BackfillKey   string
}

type runMetaKey struct{}

func WithRunMeta(ctx context.Context, meta RunMeta) context.Context {
	return context.WithValue(ctx, runMetaKey{}, meta)
}

func RunMetaFromContext(ctx context.Context) (RunMeta, bool) {
	if ctx == nil {
		return RunMeta{}, false
	}
	meta, ok := ctx.Value(runMetaKey{}).(RunMeta)
	return meta, ok
}

func RunPartitionMetaFromContext(ctx context.Context) (RunPartitionMeta, bool) {
	meta, ok := RunMetaFromContext(ctx)
	if !ok || meta.Partition == nil {
		return RunPartitionMeta{}, false
	}
	return *meta.Partition, true
}

type InputResolutionMode string

const (
	InputResolutionSingular InputResolutionMode = "singular"
	InputResolutionSlice    InputResolutionMode = "slice"
	InputResolutionPointer  InputResolutionMode = "pointer"
)

type InputBinding struct {
	FieldName  string              `json:"field_name"`
	FieldType  string              `json:"field_type"`
	TargetType string              `json:"target_type"`
	Mode       InputResolutionMode `json:"mode"`
	Providers  []string            `json:"providers"`
}

type StepDefinition struct {
	Key         string
	DisplayName string
	Description string
	DependsOn   []string
	Bindings    []InputBinding

	inputType           reflect.Type
	outputType          reflect.Type
	runValue            reflect.Value
	partitionDefinition PartitionDefinition
	partitionAssets     []string
}

func (s StepDefinition) newInputValue() reflect.Value {
	return reflect.New(s.inputType).Elem()
}

func (s StepDefinition) invoke(ctx context.Context, inValue reflect.Value) (reflect.Value, error) {
	results := s.runValue.Call([]reflect.Value{reflect.ValueOf(ctx), inValue})
	if len(results) != 2 {
		return reflect.Value{}, ErrContract("step runtime returned unexpected values")
	}
	if !results[1].IsNil() {
		err, ok := results[1].Interface().(error)
		if !ok {
			return reflect.Value{}, ErrContract("step runtime returned non-error value")
		}
		return reflect.Value{}, err
	}
	return results[0], nil
}

type ScheduleDefinition struct {
	Key         string
	CronExpr    string
	Timezone    string
	Enabled     bool
	Description string
}

type JobDefinition struct {
	Key               string
	DisplayName       string
	Description       string
	DefaultParamsJSON string
	Steps             []StepDefinition
	Schedules         []ScheduleDefinition
}
