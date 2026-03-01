package dag

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type NoInput struct{}

type StepFn[I any, O any] func(ctx context.Context, in I) (O, error)

type contractError struct {
	message string
}

func (e contractError) Error() string {
	return e.message
}

func ErrContract(message string) error {
	return contractError{message: message}
}

type nodeSpec struct {
	key         string
	displayName string
	description string
	inType      reflect.Type
	outType     reflect.Type
	runValue    reflect.Value
}

type Node[I any, O any] struct {
	spec nodeSpec
}

func Define[I any, O any](key string, fn StepFn[I, O]) Node[I, O] {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		panic("dag.Define: key is required")
	}
	inType := typeOf[I]()
	outType := typeOf[O]()
	if inType == nil || inType.Kind() != reflect.Struct {
		panic("dag.Define: input type must be a struct")
	}
	if outType == nil || outType.Kind() != reflect.Struct {
		panic("dag.Define: output type must be a struct")
	}
	runValue := reflect.ValueOf(fn)
	if !runValue.IsValid() || runValue.Kind() != reflect.Func {
		panic("dag.Define: step function is invalid")
	}
	if runValue.IsNil() {
		panic("dag.Define: step function is nil")
	}

	return Node[I, O]{
		spec: nodeSpec{
			key:         trimmedKey,
			displayName: trimmedKey,
			description: "",
			inType:      inType,
			outType:     outType,
			runValue:    runValue,
		},
	}
}

func (n Node[I, O]) WithDisplayName(displayName string) Node[I, O] {
	n.spec.displayName = strings.TrimSpace(displayName)
	if n.spec.displayName == "" {
		n.spec.displayName = n.spec.key
	}
	return n
}

func (n Node[I, O]) WithDescription(description string) Node[I, O] {
	n.spec.description = strings.TrimSpace(description)
	return n
}

func (n Node[I, O]) NodeSpec() nodeSpec {
	return n.spec
}

type nodeSpecProvider interface {
	NodeSpec() nodeSpec
}

type JobBuilder struct {
	key               string
	displayName       string
	description       string
	defaultParamsJSON string
	schedules         []ScheduleDefinition
	nodes             []nodeSpec
}

func NewJob(key string) *JobBuilder {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		panic("dag.NewJob: key is required")
	}
	return &JobBuilder{
		key:               trimmedKey,
		displayName:       trimmedKey,
		defaultParamsJSON: "{}",
		schedules:         []ScheduleDefinition{},
		nodes:             []nodeSpec{},
	}
}

func (b *JobBuilder) WithDisplayName(displayName string) *JobBuilder {
	b.displayName = strings.TrimSpace(displayName)
	if b.displayName == "" {
		b.displayName = b.key
	}
	return b
}

func (b *JobBuilder) WithDescription(description string) *JobBuilder {
	b.description = strings.TrimSpace(description)
	return b
}

func (b *JobBuilder) WithDefaultParamsJSON(defaultParamsJSON string) *JobBuilder {
	trimmed := strings.TrimSpace(defaultParamsJSON)
	if trimmed == "" {
		trimmed = "{}"
	}
	b.defaultParamsJSON = trimmed
	return b
}

func (b *JobBuilder) AddSchedule(schedule ScheduleDefinition) *JobBuilder {
	if strings.TrimSpace(schedule.Timezone) == "" {
		schedule.Timezone = "UTC"
	}
	schedule.Key = resolveScheduleKey(schedule, b.schedules)
	b.schedules = append(b.schedules, schedule)
	return b
}

func (b *JobBuilder) Add(nodes ...any) *JobBuilder {
	for _, raw := range nodes {
		provider, ok := raw.(nodeSpecProvider)
		if !ok {
			panic("dag.JobBuilder.Add: expected dag.Node values")
		}
		b.nodes = append(b.nodes, provider.NodeSpec())
	}
	return b
}

func (b *JobBuilder) MustBuild() JobDefinition {
	job, err := b.Build()
	if err != nil {
		panic(err)
	}
	return job
}

func (b *JobBuilder) Build() (JobDefinition, error) {
	if len(b.nodes) == 0 {
		return JobDefinition{}, fmt.Errorf("job %q has no steps", b.key)
	}
	steps, err := inferSteps(b.key, b.nodes)
	if err != nil {
		return JobDefinition{}, err
	}
	job := JobDefinition{
		Key:               b.key,
		DisplayName:       b.displayName,
		Description:       b.description,
		DefaultParamsJSON: b.defaultParamsJSON,
		Steps:             steps,
		Schedules:         append([]ScheduleDefinition(nil), b.schedules...),
	}
	if _, err := topologicalOrder(job); err != nil {
		return JobDefinition{}, fmt.Errorf("job %q invalid DAG: %w", b.key, err)
	}
	return job, nil
}

func inferSteps(jobKey string, nodes []nodeSpec) ([]StepDefinition, error) {
	providersByType := make(map[reflect.Type][]string)
	seenKeys := make(map[string]bool)
	steps := make([]StepDefinition, 0, len(nodes))

	for _, spec := range nodes {
		if strings.TrimSpace(spec.key) == "" {
			return nil, fmt.Errorf("job %q includes step with empty key", jobKey)
		}
		if seenKeys[spec.key] {
			return nil, fmt.Errorf("job %q includes duplicate step key %q", jobKey, spec.key)
		}
		seenKeys[spec.key] = true

		if spec.inType == nil || spec.inType.Kind() != reflect.Struct {
			return nil, fmt.Errorf("job %q step %q input type must be a struct", jobKey, spec.key)
		}
		if spec.outType == nil || spec.outType.Kind() != reflect.Struct {
			return nil, fmt.Errorf("job %q step %q output type must be a struct", jobKey, spec.key)
		}
		if !spec.runValue.IsValid() {
			return nil, fmt.Errorf("job %q step %q has invalid runtime", jobKey, spec.key)
		}

		displayName := spec.displayName
		if strings.TrimSpace(displayName) == "" {
			displayName = spec.key
		}

		steps = append(steps, StepDefinition{
			Key:         spec.key,
			DisplayName: displayName,
			Description: spec.description,
			DependsOn:   []string{},
			Bindings:    []InputBinding{},
			inputType:   spec.inType,
			outputType:  spec.outType,
			runValue:    spec.runValue,
		})
		providersByType[spec.outType] = append(providersByType[spec.outType], spec.key)
	}

	for idx := range steps {
		step := &steps[idx]
		if step.inputType == noInputType() {
			continue
		}
		for fieldIdx := 0; fieldIdx < step.inputType.NumField(); fieldIdx++ {
			field := step.inputType.Field(fieldIdx)
			if field.PkgPath != "" {
				return nil, fmt.Errorf("job %q step %q has unexported input field %q", jobKey, step.Key, field.Name)
			}

			mode, targetType, err := classifyInputFieldType(field.Type)
			if err != nil {
				return nil, fmt.Errorf("job %q step %q input field %q: %w", jobKey, step.Key, field.Name, err)
			}

			candidates := providerCandidates(providersByType[targetType], step.Key)
			sortedCandidates := append([]string(nil), candidates...)
			sort.Strings(sortedCandidates)

			switch mode {
			case InputResolutionSingular:
				if len(candidates) == 0 {
					return nil, fmt.Errorf("job %q step %q input field %q (%s) requires exactly 1 provider, found 0", jobKey, step.Key, field.Name, targetType.String())
				}
				if len(candidates) > 1 {
					return nil, fmt.Errorf("job %q step %q input field %q (%s) is ambiguous: expected exactly 1 provider, found %d (%s)", jobKey, step.Key, field.Name, targetType.String(), len(candidates), strings.Join(sortedCandidates, ", "))
				}
				provider := candidates[0]
				if !containsString(step.DependsOn, provider) {
					step.DependsOn = append(step.DependsOn, provider)
				}
				step.Bindings = append(step.Bindings, InputBinding{
					FieldName:  field.Name,
					FieldType:  field.Type.String(),
					TargetType: targetType.String(),
					Mode:       InputResolutionSingular,
					Providers:  []string{provider},
				})
			case InputResolutionPointer:
				if len(candidates) > 1 {
					return nil, fmt.Errorf("job %q step %q input field %q (%s) is ambiguous: pointer input allows 0 or 1 provider, found %d (%s)", jobKey, step.Key, field.Name, targetType.String(), len(candidates), strings.Join(sortedCandidates, ", "))
				}
				providers := []string{}
				if len(candidates) == 1 {
					provider := candidates[0]
					providers = []string{provider}
					if !containsString(step.DependsOn, provider) {
						step.DependsOn = append(step.DependsOn, provider)
					}
				}
				step.Bindings = append(step.Bindings, InputBinding{
					FieldName:  field.Name,
					FieldType:  field.Type.String(),
					TargetType: targetType.String(),
					Mode:       InputResolutionPointer,
					Providers:  providers,
				})
			case InputResolutionSlice:
				providers := append([]string(nil), candidates...)
				for _, provider := range providers {
					if !containsString(step.DependsOn, provider) {
						step.DependsOn = append(step.DependsOn, provider)
					}
				}
				step.Bindings = append(step.Bindings, InputBinding{
					FieldName:  field.Name,
					FieldType:  field.Type.String(),
					TargetType: targetType.String(),
					Mode:       InputResolutionSlice,
					Providers:  providers,
				})
			default:
				return nil, fmt.Errorf("job %q step %q input field %q has unsupported resolution mode %q", jobKey, step.Key, field.Name, mode)
			}
		}
		sort.Strings(step.DependsOn)
	}

	topo, err := topologicalOrder(JobDefinition{Key: jobKey, Steps: steps})
	if err != nil {
		return nil, fmt.Errorf("job %q invalid DAG: %w", jobKey, err)
	}
	indexByStepKey := make(map[string]int, len(topo))
	for idx, step := range topo {
		indexByStepKey[step.Key] = idx
	}
	for idx := range steps {
		for bindingIdx := range steps[idx].Bindings {
			binding := &steps[idx].Bindings[bindingIdx]
			if binding.Mode != InputResolutionSlice || len(binding.Providers) <= 1 {
				continue
			}
			sort.SliceStable(binding.Providers, func(i, j int) bool {
				left := binding.Providers[i]
				right := binding.Providers[j]
				leftIdx := indexByStepKey[left]
				rightIdx := indexByStepKey[right]
				if leftIdx == rightIdx {
					return left < right
				}
				return leftIdx < rightIdx
			})
		}
	}

	return steps, nil
}

func resolveScheduleKey(schedule ScheduleDefinition, existing []ScheduleDefinition) string {
	explicitKey := strings.TrimSpace(schedule.Key)
	if explicitKey != "" {
		return explicitKey
	}

	baseKey := inferScheduleKey(schedule.CronExpr)
	used := make(map[string]struct{}, len(existing))
	for _, item := range existing {
		key := strings.TrimSpace(item.Key)
		if key == "" {
			continue
		}
		used[key] = struct{}{}
	}
	if _, ok := used[baseKey]; !ok {
		return baseKey
	}
	for suffix := 2; ; suffix++ {
		candidate := baseKey + "_" + strconv.Itoa(suffix)
		if _, ok := used[candidate]; !ok {
			return candidate
		}
	}
}

func inferScheduleKey(cronExpr string) string {
	trimmed := strings.ToLower(strings.TrimSpace(cronExpr))
	switch trimmed {
	case "":
		return "schedule"
	case "@yearly", "@annually":
		return "yearly"
	case "@monthly":
		return "monthly"
	case "@weekly":
		return "weekly"
	case "@daily", "@midnight":
		return "daily"
	case "@hourly":
		return "hourly"
	}

	fields := strings.Fields(trimmed)
	if len(fields) != 5 {
		return "cron_" + slugifyScheduleToken(trimmed)
	}

	minute, hour, dom, month, dow := fields[0], fields[1], fields[2], fields[3], fields[4]
	switch {
	case minute == "*" && hour == "*" && dom == "*" && month == "*" && dow == "*":
		return "every_minute"
	case strings.HasPrefix(minute, "*/") && hour == "*" && dom == "*" && month == "*" && dow == "*":
		step := strings.TrimPrefix(minute, "*/")
		if isDigits(step) {
			return "every_" + step + "_minutes"
		}
	case isDigits(minute) && hour == "*" && dom == "*" && month == "*" && dow == "*":
		if minute == "0" {
			return "hourly"
		}
		return "hourly_at_" + zeroPad(minute)
	case isDigits(minute) && strings.HasPrefix(hour, "*/") && dom == "*" && month == "*" && dow == "*":
		step := strings.TrimPrefix(hour, "*/")
		if isDigits(step) {
			return "every_" + step + "_hours_at_" + zeroPad(minute)
		}
	case isDigits(minute) && isDigits(hour) && dom == "*" && month == "*" && dow == "*":
		if minute == "0" && hour == "0" {
			return "daily"
		}
		return "daily_at_" + zeroPad(hour) + "_" + zeroPad(minute)
	case isDigits(minute) && isDigits(hour) && dom == "*" && month == "*" && weekdayKey(dow) != "":
		return "weekly_on_" + weekdayKey(dow) + "_at_" + zeroPad(hour) + "_" + zeroPad(minute)
	case isDigits(minute) && isDigits(hour) && isDigits(dom) && month == "*" && dow == "*":
		if dom == "1" && minute == "0" && hour == "0" {
			return "monthly"
		}
		return "monthly_on_" + dom + "_at_" + zeroPad(hour) + "_" + zeroPad(minute)
	}

	return "cron_" + slugifyScheduleToken(strings.Join(fields, "_"))
}

func slugifyScheduleToken(value string) string {
	var b strings.Builder
	lastUnderscore := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastUnderscore = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if lastUnderscore {
				continue
			}
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "schedule"
	}
	return out
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func zeroPad(value string) string {
	if !isDigits(value) {
		return value
	}
	if len(value) >= 2 {
		return value
	}
	return "0" + value
}

func weekdayKey(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0", "7", "sun":
		return "sun"
	case "1", "mon":
		return "mon"
	case "2", "tue":
		return "tue"
	case "3", "wed":
		return "wed"
	case "4", "thu":
		return "thu"
	case "5", "fri":
		return "fri"
	case "6", "sat":
		return "sat"
	default:
		return ""
	}
}

func classifyInputFieldType(fieldType reflect.Type) (InputResolutionMode, reflect.Type, error) {
	if fieldType == nil {
		return "", nil, ErrContract("input field type is nil")
	}
	switch fieldType.Kind() {
	case reflect.Slice:
		target := fieldType.Elem()
		if target == nil || target.Kind() != reflect.Struct {
			return "", nil, fmt.Errorf("unsupported slice input type %s: expected []T where T is a struct", fieldType.String())
		}
		return InputResolutionSlice, target, nil
	case reflect.Pointer:
		target := fieldType.Elem()
		if target == nil || target.Kind() != reflect.Struct {
			return "", nil, fmt.Errorf("unsupported pointer input type %s: expected *T where T is a struct", fieldType.String())
		}
		return InputResolutionPointer, target, nil
	default:
		if fieldType.Kind() != reflect.Struct {
			return "", nil, fmt.Errorf("unsupported singular input type %s: expected T where T is a struct", fieldType.String())
		}
		return InputResolutionSingular, fieldType, nil
	}
}

func providerCandidates(providers []string, currentStepKey string) []string {
	out := make([]string, 0, len(providers))
	for _, provider := range providers {
		if provider == currentStepKey {
			continue
		}
		out = append(out, provider)
	}
	return out
}

func typeOf[T any]() reflect.Type {
	var zero T
	return reflect.TypeOf(zero)
}

func noInputType() reflect.Type {
	return reflect.TypeOf(NoInput{})
}

func containsString(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}
