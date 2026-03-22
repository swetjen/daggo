package queues

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/daggo/queue"
	"github.com/swetjen/virtuous/rpc"
)

type Handlers struct {
	app *deps.Deps
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{app: app}
}

type QueueJob struct {
	JobKey      string `json:"job_key"`
	DisplayName string `json:"display_name"`
}

type QueueSummary struct {
	ID                   int64      `json:"id"`
	QueueKey             string     `json:"queue_key"`
	DisplayName          string     `json:"display_name"`
	Description          string     `json:"description"`
	RoutePath            string     `json:"route_path"`
	LoadMode             string     `json:"load_mode"`
	LoadPollEverySeconds int64      `json:"load_poll_every_seconds"`
	Jobs                 []QueueJob `json:"jobs"`
	ItemCount            int64      `json:"item_count"`
	PartitionCount       int64      `json:"partition_count"`
	QueuedCount          int64      `json:"queued_count"`
	RunningCount         int64      `json:"running_count"`
	CompleteCount        int64      `json:"complete_count"`
	FailedCount          int64      `json:"failed_count"`
}

type QueueItemSummary struct {
	ID           int64  `json:"id"`
	QueueItemKey string `json:"queue_item_key"`
	PartitionKey string `json:"partition_key"`
	Status       string `json:"status"`
	ExternalKey  string `json:"external_key"`
	ErrorMessage string `json:"error_message"`
	QueuedAt     string `json:"queued_at"`
	StartedAt    string `json:"started_at"`
	CompletedAt  string `json:"completed_at"`
}

type QueuePartitionSummary struct {
	LatestQueueItemID int64  `json:"latest_queue_item_id"`
	QueueItemKey      string `json:"queue_item_key"`
	PartitionKey      string `json:"partition_key"`
	Status            string `json:"status"`
	ExternalKey       string `json:"external_key"`
	ErrorMessage      string `json:"error_message"`
	QueuedAt          string `json:"queued_at"`
	StartedAt         string `json:"started_at"`
	CompletedAt       string `json:"completed_at"`
}

type QueueLinkedRun struct {
	ID             int64  `json:"id"`
	RunKey         string `json:"run_key"`
	JobKey         string `json:"job_key"`
	JobDisplayName string `json:"job_display_name"`
	Status         string `json:"status"`
	TriggeredBy    string `json:"triggered_by"`
	QueuedAt       string `json:"queued_at"`
	StartedAt      string `json:"started_at"`
	CompletedAt    string `json:"completed_at"`
	ErrorMessage   string `json:"error_message"`
}

type QueueItemDetail struct {
	ID               int64                        `json:"id"`
	QueueID          int64                        `json:"queue_id"`
	QueueKey         string                       `json:"queue_key"`
	QueueDisplayName string                       `json:"queue_display_name"`
	QueueItemKey     string                       `json:"queue_item_key"`
	PartitionKey     string                       `json:"partition_key"`
	Status           string                       `json:"status"`
	ExternalKey      string                       `json:"external_key"`
	PayloadJSON      string                       `json:"payload_json"`
	ErrorMessage     string                       `json:"error_message"`
	QueuedAt         string                       `json:"queued_at"`
	StartedAt        string                       `json:"started_at"`
	CompletedAt      string                       `json:"completed_at"`
	Metadata         map[string]map[string]any    `json:"metadata"`
}

type QueuesGetManyRequest struct {
	Limit  int64 `json:"limit"`
	Offset int64 `json:"offset"`
}

type QueuesGetManyResponse struct {
	Data  []QueueSummary `json:"data"`
	Total int64          `json:"total"`
	Error string         `json:"error,omitempty"`
}

type QueueByKeyRequest struct {
	QueueKey string `json:"queue_key"`
}

type QueueByKeyResponse struct {
	Queue QueueSummary `json:"queue"`
	Error string       `json:"error,omitempty"`
}

type QueueItemsGetManyRequest struct {
	QueueKey string `json:"queue_key"`
	Limit    int64  `json:"limit"`
	Offset   int64  `json:"offset"`
}

type QueueItemsGetManyResponse struct {
	Data  []QueueItemSummary `json:"data"`
	Total int64              `json:"total"`
	Error string             `json:"error,omitempty"`
}

type QueuePartitionsGetManyRequest struct {
	QueueKey string `json:"queue_key"`
	Limit    int64  `json:"limit"`
	Offset   int64  `json:"offset"`
}

type QueuePartitionsGetManyResponse struct {
	Data  []QueuePartitionSummary `json:"data"`
	Total int64                   `json:"total"`
	Error string                  `json:"error,omitempty"`
}

type QueueItemByIDRequest struct {
	ID int64 `json:"id"`
}

type QueueItemByIDResponse struct {
	Item  QueueItemDetail  `json:"item"`
	Runs  []QueueLinkedRun `json:"runs"`
	Error string           `json:"error,omitempty"`
}

func (h *Handlers) QueuesGetMany(ctx context.Context, req QueuesGetManyRequest) (QueuesGetManyResponse, int) {
	limit, offset := normalizePagination(req.Limit, req.Offset)
	definitions := currentQueues(h.app)
	total := int64(len(definitions))
	start := minInt(int(offset), len(definitions))
	end := minInt(start+int(limit), len(definitions))

	out := make([]QueueSummary, 0, end-start)
	for _, definition := range definitions[start:end] {
		summary, err := h.loadQueueSummary(ctx, definition)
		if err != nil {
			return QueuesGetManyResponse{Error: "failed to load queues"}, rpc.StatusError
		}
		out = append(out, summary)
	}
	return QueuesGetManyResponse{Data: out, Total: total}, rpc.StatusOK
}

func (h *Handlers) QueueByKey(ctx context.Context, req QueueByKeyRequest) (QueueByKeyResponse, int) {
	definition, ok := currentQueueByKey(h.app, req.QueueKey)
	if !ok {
		return QueueByKeyResponse{Error: "queue_key is required"}, rpc.StatusInvalid
	}
	summary, err := h.loadQueueSummary(ctx, definition)
	if err != nil {
		return QueueByKeyResponse{Error: "failed to load queue"}, rpc.StatusError
	}
	return QueueByKeyResponse{Queue: summary}, rpc.StatusOK
}

func (h *Handlers) QueueItemsGetMany(ctx context.Context, req QueueItemsGetManyRequest) (QueueItemsGetManyResponse, int) {
	definition, ok := currentQueueByKey(h.app, req.QueueKey)
	if !ok {
		return QueueItemsGetManyResponse{Error: "queue_key is required"}, rpc.StatusInvalid
	}
	queueRow, err := h.app.DB.QueueGetByKey(ctx, definition.Key)
	if err != nil {
		return QueueItemsGetManyResponse{Error: "failed to load queue"}, rpc.StatusError
	}
	limit, offset := normalizePagination(req.Limit, req.Offset)
	rows, err := h.app.DB.QueueItemGetManyByQueueID(ctx, db.QueueItemGetManyByQueueIDParams{
		QueueID: queueRow.ID,
		Limit:   limit,
		Offset:  offset,
	})
	if err != nil {
		return QueueItemsGetManyResponse{Error: "failed to load queue items"}, rpc.StatusError
	}
	total, err := h.app.DB.QueueItemCountByQueueID(ctx, queueRow.ID)
	if err != nil {
		return QueueItemsGetManyResponse{Error: "failed to count queue items"}, rpc.StatusError
	}
	return QueueItemsGetManyResponse{
		Data:  toQueueItemSummaries(rows),
		Total: total,
	}, rpc.StatusOK
}

func (h *Handlers) QueuePartitionsGetMany(ctx context.Context, req QueuePartitionsGetManyRequest) (QueuePartitionsGetManyResponse, int) {
	definition, ok := currentQueueByKey(h.app, req.QueueKey)
	if !ok {
		return QueuePartitionsGetManyResponse{Error: "queue_key is required"}, rpc.StatusInvalid
	}
	queueRow, err := h.app.DB.QueueGetByKey(ctx, definition.Key)
	if err != nil {
		return QueuePartitionsGetManyResponse{Error: "failed to load queue"}, rpc.StatusError
	}
	limit, offset := normalizePagination(req.Limit, req.Offset)
	rows, err := h.app.DB.QueuePartitionGetManyByQueueID(ctx, db.QueuePartitionGetManyByQueueIDParams{
		QueueID: queueRow.ID,
		Limit:   limit,
		Offset:  offset,
	})
	if err != nil {
		return QueuePartitionsGetManyResponse{Error: "failed to load queue partitions"}, rpc.StatusError
	}
	total, err := h.app.DB.QueuePartitionCountByQueueID(ctx, queueRow.ID)
	if err != nil {
		return QueuePartitionsGetManyResponse{Error: "failed to count queue partitions"}, rpc.StatusError
	}
	return QueuePartitionsGetManyResponse{
		Data:  toQueuePartitionSummaries(rows),
		Total: total,
	}, rpc.StatusOK
}

func (h *Handlers) QueueItemByID(ctx context.Context, req QueueItemByIDRequest) (QueueItemByIDResponse, int) {
	if req.ID <= 0 {
		return QueueItemByIDResponse{Error: "id is required"}, rpc.StatusInvalid
	}
	row, err := h.app.DB.QueueItemGetByIDJoinedQueues(ctx, req.ID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return QueueItemByIDResponse{Error: "queue item not found"}, rpc.StatusInvalid
		}
		return QueueItemByIDResponse{Error: "failed to load queue item"}, rpc.StatusError
	}
	runs, err := h.app.DB.QueueItemRunGetManyByQueueItemIDJoinedRuns(ctx, row.ID)
	if err != nil {
		return QueueItemByIDResponse{Error: "failed to load linked runs"}, rpc.StatusError
	}
	metadataRows, err := h.app.DB.QueueItemStepMetadataGetManyByQueueItemID(ctx, row.ID)
	if err != nil {
		return QueueItemByIDResponse{Error: "failed to load metadata"}, rpc.StatusError
	}
	return QueueItemByIDResponse{
		Item: QueueItemDetail{
			ID:               row.ID,
			QueueID:          row.QueueID,
			QueueKey:         row.QueueKey,
			QueueDisplayName: row.QueueDisplayName,
			QueueItemKey:     row.QueueItemKey,
			PartitionKey:     row.PartitionKey,
			Status:           row.Status,
			ExternalKey:      row.ExternalKey,
			PayloadJSON:      row.PayloadJson,
			ErrorMessage:     row.ErrorMessage,
			QueuedAt:         row.QueuedAt,
			StartedAt:        row.StartedAt,
			CompletedAt:      row.CompletedAt,
			Metadata:         aggregateMetadata(runs, metadataRows),
		},
		Runs: toQueueLinkedRuns(runs),
	}, rpc.StatusOK
}

func (h *Handlers) loadQueueSummary(ctx context.Context, definition queue.Definition) (QueueSummary, error) {
	row, err := h.app.DB.QueueGetByKey(ctx, definition.Key)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return QueueSummary{
				QueueKey:             definition.Key,
				DisplayName:          definition.DisplayName,
				Description:          definition.Description,
				RoutePath:            definition.RoutePath,
				LoadMode:             string(definition.LoadMode),
				LoadPollEverySeconds: int64(definition.LoadPollEvery.Seconds()),
				Jobs:                 toQueueJobs(definition.Jobs()),
			}, nil
		}
		return QueueSummary{}, err
	}
	statusCounts, err := h.app.DB.QueueItemStatusCountGetManyByQueueID(ctx, row.ID)
	if err != nil {
		return QueueSummary{}, err
	}
	itemCount, err := h.app.DB.QueueItemCountByQueueID(ctx, row.ID)
	if err != nil {
		return QueueSummary{}, err
	}
	partitionCount, err := h.app.DB.QueuePartitionCountByQueueID(ctx, row.ID)
	if err != nil {
		return QueueSummary{}, err
	}
	queuedCount, runningCount, completeCount, failedCount := countStatuses(statusCounts)
	return QueueSummary{
		ID:                   row.ID,
		QueueKey:             definition.Key,
		DisplayName:          definition.DisplayName,
		Description:          definition.Description,
		RoutePath:            definition.RoutePath,
		LoadMode:             string(definition.LoadMode),
		LoadPollEverySeconds: int64(definition.LoadPollEvery.Seconds()),
		Jobs:                 toQueueJobs(definition.Jobs()),
		ItemCount:            itemCount,
		PartitionCount:       partitionCount,
		QueuedCount:          queuedCount,
		RunningCount:         runningCount,
		CompleteCount:        completeCount,
		FailedCount:          failedCount,
	}, nil
}

func currentQueues(app *deps.Deps) []queue.Definition {
	if app == nil || app.Queues == nil {
		return nil
	}
	return app.Queues.Queues()
}

func currentQueueByKey(app *deps.Deps, queueKey string) (queue.Definition, bool) {
	if app == nil || app.Queues == nil {
		return queue.Definition{}, false
	}
	trimmed := strings.TrimSpace(queueKey)
	if trimmed == "" {
		return queue.Definition{}, false
	}
	return app.Queues.QueueByKey(trimmed)
}

func toQueueJobs(jobs []dag.JobDefinition) []QueueJob {
	out := make([]QueueJob, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, QueueJob{
			JobKey:      job.Key,
			DisplayName: job.DisplayName,
		})
	}
	return out
}

func toQueueItemSummaries(rows []db.QueueItem) []QueueItemSummary {
	out := make([]QueueItemSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, QueueItemSummary{
			ID:           row.ID,
			QueueItemKey: row.QueueItemKey,
			PartitionKey: row.PartitionKey,
			Status:       row.Status,
			ExternalKey:  row.ExternalKey,
			ErrorMessage: row.ErrorMessage,
			QueuedAt:     row.QueuedAt,
			StartedAt:    row.StartedAt,
			CompletedAt:  row.CompletedAt,
		})
	}
	return out
}

func toQueuePartitionSummaries(rows []db.QueuePartitionGetManyByQueueIDRow) []QueuePartitionSummary {
	out := make([]QueuePartitionSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, QueuePartitionSummary{
			LatestQueueItemID: row.ID,
			QueueItemKey:      row.QueueItemKey,
			PartitionKey:      row.PartitionKey,
			Status:            row.Status,
			ExternalKey:       row.ExternalKey,
			ErrorMessage:      row.ErrorMessage,
			QueuedAt:          row.QueuedAt,
			StartedAt:         row.StartedAt,
			CompletedAt:       row.CompletedAt,
		})
	}
	return out
}

func toQueueLinkedRuns(rows []db.QueueItemRunGetManyByQueueItemIDJoinedRunsRow) []QueueLinkedRun {
	out := make([]QueueLinkedRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, QueueLinkedRun{
			ID:             row.RunID,
			RunKey:         row.RunKey,
			JobKey:         row.JobKey,
			JobDisplayName: row.JobDisplayName,
			Status:         row.RunStatus,
			TriggeredBy:    row.TriggeredBy,
			QueuedAt:       row.QueuedAt,
			StartedAt:      row.StartedAt,
			CompletedAt:    row.CompletedAt,
			ErrorMessage:   row.RunErrorMessage,
		})
	}
	return out
}

func aggregateMetadata(runRows []db.QueueItemRunGetManyByQueueItemIDJoinedRunsRow, metadataRows []db.QueueItemStepMetadatum) map[string]map[string]any {
	jobKeysByID := make(map[int64]string, len(runRows))
	for _, row := range runRows {
		jobKeysByID[row.JobID] = row.JobKey
	}
	out := make(map[string]map[string]any)
	for _, row := range metadataRows {
		jobKey := jobKeysByID[row.JobID]
		if jobKey == "" {
			jobKey = "job"
		}
		stepMap := out[jobKey]
		if stepMap == nil {
			stepMap = make(map[string]any)
			out[jobKey] = stepMap
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(row.MetadataJson), &payload); err != nil {
			payload = map[string]any{}
		}
		stepMap[row.StepKey] = payload
	}
	return out
}

func countStatuses(rows []db.QueueItemStatusCountGetManyByQueueIDRow) (int64, int64, int64, int64) {
	var queuedCount, runningCount, completeCount, failedCount int64
	for _, row := range rows {
		switch row.Status {
		case "queued":
			queuedCount += row.Total
		case "running":
			runningCount += row.Total
		case "complete":
			completeCount += row.Total
		case "failed":
			failedCount += row.Total
		}
	}
	return queuedCount, runningCount, completeCount, failedCount
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func normalizePagination(limit, offset int64) (int64, int64) {
	if limit <= 0 {
		limit = 25
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
