package daggo

import (
	"context"
	"encoding/json"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/queue"
)

type QueueDefinition = queue.Definition
type QueueBuilder[T any] = queue.Builder[T]
type LoadedQueueItem[T any] = queue.LoadedItem[T]
type QueueLoaderFunc[T any] = queue.LoaderFunc[T]
type QueueLoadMode = queue.LoadMode
type QueueLoaderOptions = queue.LoaderOptions
type MetadataProvider = dag.MetadataProvider
type RunQueueMeta = dag.RunQueueMeta

const (
	QueueLoadModePoll   = queue.LoadModePoll
	QueueLoadModeStream = queue.LoadModeStream
)

func NewQueue[T any](key string) *queue.Builder[T] {
	return queue.New[T](key)
}

func QueueMetaFromContext(ctx context.Context) (dag.RunQueueMeta, bool) {
	return dag.RunQueueMetaFromContext(ctx)
}

func QueuePayloadFromContext[T any](ctx context.Context) (T, bool, error) {
	meta, ok := dag.RunQueueMetaFromContext(ctx)
	if !ok {
		var zero T
		return zero, false, nil
	}
	payloadBytes, err := json.Marshal(meta.Payload)
	if err != nil {
		var zero T
		return zero, true, err
	}
	var out T
	if err := json.Unmarshal(payloadBytes, &out); err != nil {
		var zero T
		return zero, true, err
	}
	return out, true, nil
}
