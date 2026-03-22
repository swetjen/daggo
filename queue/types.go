package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/swetjen/daggo/dag"
)

type LoadMode string

const (
	LoadModePoll   LoadMode = "poll"
	LoadModeStream LoadMode = "stream"
)

type LoadedItem[T any] struct {
	Value       T
	ExternalKey string
	QueuedAt    time.Time
}

type LoaderFunc[T any] func(ctx context.Context, emit func(LoadedItem[T]) error) error

type LoaderOptions struct {
	Mode      LoadMode
	PollEvery time.Duration
}

type runtimeLoadedItem struct {
	Value       any
	ExternalKey string
	QueuedAt    time.Time
}

type Definition struct {
	Key              string
	DisplayName      string
	Description      string
	RoutePath        string
	LoadMode         LoadMode
	LoadPollEvery    time.Duration
	routeHandler     http.Handler
	jobs             []dag.JobDefinition
	load             func(context.Context, func(runtimeLoadedItem) error) error
	resolvePartition func(any) (string, error)
	marshalPayload   func(any) (string, error)
}

func (d Definition) Jobs() []dag.JobDefinition {
	if len(d.jobs) == 0 {
		return nil
	}
	out := make([]dag.JobDefinition, len(d.jobs))
	copy(out, d.jobs)
	return out
}

func (d Definition) JobKeys() []string {
	if len(d.jobs) == 0 {
		return nil
	}
	out := make([]string, 0, len(d.jobs))
	for _, job := range d.jobs {
		out = append(out, job.Key)
	}
	return out
}

func (d Definition) RouteHandler() http.Handler {
	return d.routeHandler
}

func (d Definition) Load(ctx context.Context, emit func(runtimeLoadedItem) error) error {
	if d.load == nil {
		return fmt.Errorf("queue %q loader is not configured", d.Key)
	}
	return d.load(ctx, emit)
}

func (d Definition) ResolvePartitionKey(value any) (string, error) {
	if d.resolvePartition == nil {
		return "", fmt.Errorf("queue %q partition resolver is not configured", d.Key)
	}
	partitionKey, err := d.resolvePartition(value)
	if err != nil {
		return "", err
	}
	partitionKey = strings.TrimSpace(partitionKey)
	if partitionKey == "" {
		return "", fmt.Errorf("queue %q partition key is required", d.Key)
	}
	return partitionKey, nil
}

func (d Definition) MarshalPayloadJSON(value any) (string, error) {
	if d.marshalPayload == nil {
		return "", fmt.Errorf("queue %q payload marshaler is not configured", d.Key)
	}
	return d.marshalPayload(value)
}

type Builder[T any] struct {
	def Definition
}

func New[T any](key string) *Builder[T] {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		panic("daggo.NewQueue: key is required")
	}
	return &Builder[T]{
		def: Definition{
			Key:           trimmedKey,
			DisplayName:   trimmedKey,
			LoadMode:      LoadModePoll,
			LoadPollEvery: time.Second,
			jobs:          []dag.JobDefinition{},
		},
	}
}

func (b *Builder[T]) WithDisplayName(name string) *Builder[T] {
	b.def.DisplayName = strings.TrimSpace(name)
	if b.def.DisplayName == "" {
		b.def.DisplayName = b.def.Key
	}
	return b
}

func (b *Builder[T]) WithDescription(description string) *Builder[T] {
	b.def.Description = strings.TrimSpace(description)
	return b
}

func (b *Builder[T]) WithRoute(path string, handler http.Handler) *Builder[T] {
	b.def.RoutePath = strings.TrimSpace(path)
	b.def.routeHandler = handler
	return b
}

func (b *Builder[T]) WithLoader(loader LoaderFunc[T], opts LoaderOptions) *Builder[T] {
	if loader == nil {
		b.def.load = nil
		return b
	}
	mode := normalizeLoadMode(opts.Mode)
	pollEvery := opts.PollEvery
	if mode == LoadModePoll && pollEvery <= 0 {
		pollEvery = time.Second
	}
	b.def.LoadMode = mode
	b.def.LoadPollEvery = pollEvery
	b.def.load = func(ctx context.Context, emit func(runtimeLoadedItem) error) error {
		return loader(ctx, func(item LoadedItem[T]) error {
			return emit(runtimeLoadedItem{
				Value:       item.Value,
				ExternalKey: strings.TrimSpace(item.ExternalKey),
				QueuedAt:    item.QueuedAt,
			})
		})
	}
	return b
}

func (b *Builder[T]) WithPartitionKey(fn func(T) (string, error)) *Builder[T] {
	if fn == nil {
		b.def.resolvePartition = nil
		b.def.marshalPayload = nil
		return b
	}
	b.def.resolvePartition = func(value any) (string, error) {
		typed, ok := value.(T)
		if !ok {
			return "", fmt.Errorf("queue %q loaded item type mismatch", b.def.Key)
		}
		return fn(typed)
	}
	b.def.marshalPayload = func(value any) (string, error) {
		typed, ok := value.(T)
		if !ok {
			return "", fmt.Errorf("queue %q loaded item type mismatch", b.def.Key)
		}
		payload, err := json.Marshal(typed)
		if err != nil {
			return "", fmt.Errorf("marshal queue payload: %w", err)
		}
		return string(payload), nil
	}
	return b
}

func (b *Builder[T]) AddJobs(jobs ...dag.JobDefinition) *Builder[T] {
	b.def.jobs = append(b.def.jobs, jobs...)
	return b
}

func (b *Builder[T]) MustBuild() Definition {
	definition, err := b.Build()
	if err != nil {
		panic(err)
	}
	return definition
}

func (b *Builder[T]) Build() (Definition, error) {
	definition := b.def
	if strings.TrimSpace(definition.Key) == "" {
		return Definition{}, fmt.Errorf("queue key is required")
	}
	if strings.TrimSpace(definition.DisplayName) == "" {
		definition.DisplayName = definition.Key
	}
	if len(definition.jobs) == 0 {
		return Definition{}, fmt.Errorf("queue %q must attach at least one job", definition.Key)
	}
	if definition.load == nil {
		return Definition{}, fmt.Errorf("queue %q loader is required", definition.Key)
	}
	if definition.resolvePartition == nil {
		return Definition{}, fmt.Errorf("queue %q partition resolver is required", definition.Key)
	}
	if definition.marshalPayload == nil {
		return Definition{}, fmt.Errorf("queue %q payload marshaler is required", definition.Key)
	}
	definition.LoadMode = normalizeLoadMode(definition.LoadMode)
	if definition.LoadMode == LoadModePoll && definition.LoadPollEvery <= 0 {
		definition.LoadPollEvery = time.Second
	}
	if err := validateRoute(definition.RoutePath, definition.routeHandler); err != nil {
		return Definition{}, fmt.Errorf("queue %q route: %w", definition.Key, err)
	}
	seenJobKeys := make(map[string]struct{}, len(definition.jobs))
	for _, job := range definition.jobs {
		jobKey := strings.TrimSpace(job.Key)
		if jobKey == "" {
			return Definition{}, fmt.Errorf("queue %q includes job with empty key", definition.Key)
		}
		if _, exists := seenJobKeys[jobKey]; exists {
			return Definition{}, fmt.Errorf("queue %q includes duplicate job %q", definition.Key, jobKey)
		}
		seenJobKeys[jobKey] = struct{}{}
	}
	return definition, nil
}

func normalizeLoadMode(mode LoadMode) LoadMode {
	switch LoadMode(strings.ToLower(strings.TrimSpace(string(mode)))) {
	case "", LoadModePoll:
		return LoadModePoll
	case LoadModeStream:
		return LoadModeStream
	default:
		return LoadModePoll
	}
}

func validateRoute(path string, handler http.Handler) error {
	trimmedPath := strings.TrimSpace(path)
	if trimmedPath == "" {
		if handler != nil {
			return fmt.Errorf("route path is required when handler is set")
		}
		return nil
	}
	if handler == nil {
		return fmt.Errorf("handler is required when route path is set")
	}
	if !strings.HasPrefix(trimmedPath, "/") {
		return fmt.Errorf("route path must start with /")
	}
	normalized := strings.TrimRight(trimmedPath, "/")
	if normalized == "" {
		return fmt.Errorf("route path cannot be /")
	}
	reserved := []string{"/rpc", "/assets", "/overview", "/jobs", "/runs", "/queues"}
	for _, prefix := range reserved {
		if normalized == prefix || strings.HasPrefix(normalized, prefix+"/") {
			return fmt.Errorf("route path %q conflicts with DAGGO reserved routes", trimmedPath)
		}
	}
	return nil
}
