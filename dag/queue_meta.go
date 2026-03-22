package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const runParamsQueueKey = "daggo_queue"

type RunQueueMeta struct {
	QueueKey     string
	QueueItemID  int64
	PartitionKey string
	ExternalKey  string
	Payload      any
}

type MetadataProvider interface {
	DaggoMetadata() map[string]any
}

func QueueRunParamsMap(meta RunQueueMeta) map[string]any {
	return map[string]any{
		runParamsQueueKey: map[string]any{
			"queue_key":     strings.TrimSpace(meta.QueueKey),
			"queue_item_id": meta.QueueItemID,
			"partition_key": strings.TrimSpace(meta.PartitionKey),
			"external_key":  strings.TrimSpace(meta.ExternalKey),
			"payload":       meta.Payload,
		},
	}
}

func RunQueueMetaFromContext(ctx context.Context) (RunQueueMeta, bool) {
	meta, ok := RunMetaFromContext(ctx)
	if !ok || meta.Queue == nil {
		return RunQueueMeta{}, false
	}
	return *meta.Queue, true
}

func queueMetaFromParams(params map[string]any) (*RunQueueMeta, error) {
	if len(params) == 0 {
		return nil, nil
	}
	raw, ok := params[runParamsQueueKey]
	if !ok {
		return nil, nil
	}
	payload, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("queue params payload is invalid")
	}
	meta := RunQueueMeta{
		QueueKey:     readStringAny(payload["queue_key"]),
		QueueItemID:  readInt64Any(payload["queue_item_id"]),
		PartitionKey: readStringAny(payload["partition_key"]),
		ExternalKey:  readStringAny(payload["external_key"]),
		Payload:      payload["payload"],
	}
	if meta.QueueKey == "" || meta.QueueItemID <= 0 || meta.PartitionKey == "" {
		return nil, fmt.Errorf("queue params are incomplete")
	}
	return &meta, nil
}

func readStringAny(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func readInt64Any(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int32:
		return int64(typed)
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	case float32:
		return int64(typed)
	case json.Number:
		if parsed, err := typed.Int64(); err == nil {
			return parsed
		}
	}
	return 0
}
