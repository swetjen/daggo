package db

import (
	"context"

	"github.com/swetjen/daggo/db/postgresgen"
)

func (s *PostgresStore) QueueGetMany(ctx context.Context, arg QueueGetManyParams) ([]Queue, error) {
	rows, err := s.queries.QueueGetMany(ctx, postgresgen.QueueGetManyParams{
		Limit:  int32(arg.Limit),
		Offset: int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresQueue), nil
}

func (s *PostgresStore) QueueCount(ctx context.Context) (int64, error) {
	return s.queries.QueueCount(ctx)
}

func (s *PostgresStore) QueueGetByKey(ctx context.Context, queueKey string) (Queue, error) {
	row, err := s.queries.QueueGetByKey(ctx, queueKey)
	if err != nil {
		return Queue{}, err
	}
	return fromPostgresQueue(row), nil
}

func (s *PostgresStore) QueueUpsert(ctx context.Context, arg QueueUpsertParams) (Queue, error) {
	row, err := s.queries.QueueUpsert(ctx, postgresgen.QueueUpsertParams{
		QueueKey:             arg.QueueKey,
		DisplayName:          arg.DisplayName,
		Description:          arg.Description,
		RoutePath:            arg.RoutePath,
		LoadMode:             arg.LoadMode,
		LoadPollEverySeconds: arg.LoadPollEverySeconds,
	})
	if err != nil {
		return Queue{}, err
	}
	return fromPostgresQueue(row), nil
}

func (s *PostgresStore) QueueJobDeleteByQueueID(ctx context.Context, queueID int64) error {
	return s.queries.QueueJobDeleteByQueueID(ctx, queueID)
}

func (s *PostgresStore) QueueJobCreate(ctx context.Context, arg QueueJobCreateParams) (QueueJob, error) {
	row, err := s.queries.QueueJobCreate(ctx, postgresgen.QueueJobCreateParams{
		QueueID:   arg.QueueID,
		JobID:     arg.JobID,
		SortIndex: arg.SortIndex,
	})
	if err != nil {
		return QueueJob{}, err
	}
	return fromPostgresQueueJob(row), nil
}

func (s *PostgresStore) QueueItemCreate(ctx context.Context, arg QueueItemCreateParams) (QueueItem, error) {
	row, err := s.queries.QueueItemCreate(ctx, postgresgen.QueueItemCreateParams{
		QueueID:      arg.QueueID,
		QueueItemKey: arg.QueueItemKey,
		PartitionKey: arg.PartitionKey,
		Status:       arg.Status,
		ExternalKey:  arg.ExternalKey,
		PayloadJson:  []byte(arg.PayloadJson),
		ErrorMessage: arg.ErrorMessage,
		QueuedAt:     mustParseStoredTime(arg.QueuedAt),
		StartedAt:    toNullTime(arg.StartedAt),
		CompletedAt:  toNullTime(arg.CompletedAt),
	})
	if err != nil {
		return QueueItem{}, err
	}
	return fromPostgresQueueItem(row), nil
}

func (s *PostgresStore) QueueItemGetByIDJoinedQueues(ctx context.Context, id int64) (QueueItemGetByIDJoinedQueuesRow, error) {
	row, err := s.queries.QueueItemGetByIDJoinedQueues(ctx, id)
	if err != nil {
		return QueueItemGetByIDJoinedQueuesRow{}, err
	}
	return fromPostgresQueueItemGetByIDJoinedQueuesRow(row), nil
}

func (s *PostgresStore) QueueItemGetManyByQueueID(ctx context.Context, arg QueueItemGetManyByQueueIDParams) ([]QueueItem, error) {
	rows, err := s.queries.QueueItemGetManyByQueueID(ctx, postgresgen.QueueItemGetManyByQueueIDParams{
		QueueID: arg.QueueID,
		Limit:   int32(arg.Limit),
		Offset:  int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresQueueItem), nil
}

func (s *PostgresStore) QueueItemCountByQueueID(ctx context.Context, queueID int64) (int64, error) {
	return s.queries.QueueItemCountByQueueID(ctx, queueID)
}

func (s *PostgresStore) QueueItemUpdateLifecycleByID(ctx context.Context, arg QueueItemUpdateLifecycleByIDParams) (QueueItem, error) {
	row, err := s.queries.QueueItemUpdateLifecycleByID(ctx, postgresgen.QueueItemUpdateLifecycleByIDParams{
		Status:       arg.Status,
		StartedAt:    toNullTime(arg.StartedAt),
		CompletedAt:  toNullTime(arg.CompletedAt),
		ErrorMessage: arg.ErrorMessage,
		ID:           arg.ID,
	})
	if err != nil {
		return QueueItem{}, err
	}
	return fromPostgresQueueItem(row), nil
}

func (s *PostgresStore) QueueItemStatusCountGetManyByQueueID(ctx context.Context, queueID int64) ([]QueueItemStatusCountGetManyByQueueIDRow, error) {
	rows, err := s.queries.QueueItemStatusCountGetManyByQueueID(ctx, queueID)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresQueueItemStatusCountGetManyByQueueIDRow), nil
}

func (s *PostgresStore) QueuePartitionGetManyByQueueID(ctx context.Context, arg QueuePartitionGetManyByQueueIDParams) ([]QueuePartitionGetManyByQueueIDRow, error) {
	rows, err := s.queries.QueuePartitionGetManyByQueueID(ctx, postgresgen.QueuePartitionGetManyByQueueIDParams{
		QueueID: arg.QueueID,
		Limit:   int32(arg.Limit),
		Offset:  int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresQueuePartitionGetManyByQueueIDRow), nil
}

func (s *PostgresStore) QueuePartitionCountByQueueID(ctx context.Context, queueID int64) (int64, error) {
	return s.queries.QueuePartitionCountByQueueID(ctx, queueID)
}

func (s *PostgresStore) QueueItemRunCreate(ctx context.Context, arg QueueItemRunCreateParams) (QueueItemRun, error) {
	row, err := s.queries.QueueItemRunCreate(ctx, postgresgen.QueueItemRunCreateParams{
		QueueItemID: arg.QueueItemID,
		JobID:       arg.JobID,
		RunID:       arg.RunID,
	})
	if err != nil {
		return QueueItemRun{}, err
	}
	return fromPostgresQueueItemRun(row), nil
}

func (s *PostgresStore) QueueItemRunGetByRunID(ctx context.Context, runID int64) (QueueItemRun, error) {
	row, err := s.queries.QueueItemRunGetByRunID(ctx, runID)
	if err != nil {
		return QueueItemRun{}, err
	}
	return fromPostgresQueueItemRun(row), nil
}

func (s *PostgresStore) QueueItemRunGetManyByQueueItemIDJoinedRuns(ctx context.Context, queueItemID int64) ([]QueueItemRunGetManyByQueueItemIDJoinedRunsRow, error) {
	rows, err := s.queries.QueueItemRunGetManyByQueueItemIDJoinedRuns(ctx, queueItemID)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresQueueItemRunGetManyByQueueItemIDJoinedRunsRow), nil
}

func (s *PostgresStore) QueueItemStepMetadataUpsert(ctx context.Context, arg QueueItemStepMetadataUpsertParams) (QueueItemStepMetadatum, error) {
	row, err := s.queries.QueueItemStepMetadataUpsert(ctx, postgresgen.QueueItemStepMetadataUpsertParams{
		QueueItemID: arg.QueueItemID,
		JobID:       arg.JobID,
		RunID:       arg.RunID,
		StepKey:     arg.StepKey,
		MetadataJson: []byte(arg.MetadataJson),
	})
	if err != nil {
		return QueueItemStepMetadatum{}, err
	}
	return fromPostgresQueueItemStepMetadatum(row), nil
}

func (s *PostgresStore) QueueItemStepMetadataGetManyByQueueItemID(ctx context.Context, queueItemID int64) ([]QueueItemStepMetadatum, error) {
	rows, err := s.queries.QueueItemStepMetadataGetManyByQueueItemID(ctx, queueItemID)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresQueueItemStepMetadatum), nil
}

func fromPostgresQueue(row postgresgen.Queue) Queue {
	return Queue{
		ID:                   row.ID,
		QueueKey:             row.QueueKey,
		DisplayName:          row.DisplayName,
		Description:          row.Description,
		RoutePath:            row.RoutePath,
		LoadMode:             row.LoadMode,
		LoadPollEverySeconds: row.LoadPollEverySeconds,
		CreatedAt:            formatTime(row.CreatedAt),
		UpdatedAt:            formatTime(row.UpdatedAt),
	}
}

func fromPostgresQueueJob(row postgresgen.QueueJob) QueueJob {
	return QueueJob{
		ID:        row.ID,
		QueueID:   row.QueueID,
		JobID:     row.JobID,
		SortIndex: row.SortIndex,
		CreatedAt: formatTime(row.CreatedAt),
	}
}

func fromPostgresQueueItem(row postgresgen.QueueItem) QueueItem {
	return QueueItem{
		ID:           row.ID,
		QueueID:      row.QueueID,
		QueueItemKey: row.QueueItemKey,
		PartitionKey: row.PartitionKey,
		Status:       row.Status,
		ExternalKey:  row.ExternalKey,
		PayloadJson:  fromRawJSON(row.PayloadJson),
		ErrorMessage: row.ErrorMessage,
		QueuedAt:     formatTime(row.QueuedAt),
		StartedAt:    formatNullTime(row.StartedAt),
		CompletedAt:  formatNullTime(row.CompletedAt),
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}

func fromPostgresQueueItemRun(row postgresgen.QueueItemRun) QueueItemRun {
	return QueueItemRun{
		ID:          row.ID,
		QueueItemID: row.QueueItemID,
		JobID:       row.JobID,
		RunID:       row.RunID,
		CreatedAt:   formatTime(row.CreatedAt),
	}
}

func fromPostgresQueueItemStepMetadatum(row postgresgen.QueueItemStepMetadatum) QueueItemStepMetadatum {
	return QueueItemStepMetadatum{
		ID:           row.ID,
		QueueItemID:  row.QueueItemID,
		JobID:        row.JobID,
		RunID:        row.RunID,
		StepKey:      row.StepKey,
		MetadataJson: fromRawJSON(row.MetadataJson),
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}

func fromPostgresQueueItemGetByIDJoinedQueuesRow(row postgresgen.QueueItemGetByIDJoinedQueuesRow) QueueItemGetByIDJoinedQueuesRow {
	return QueueItemGetByIDJoinedQueuesRow{
		ID:               row.ID,
		QueueID:          row.QueueID,
		QueueItemKey:     row.QueueItemKey,
		PartitionKey:     row.PartitionKey,
		Status:           row.Status,
		ExternalKey:      row.ExternalKey,
		PayloadJson:      fromRawJSON(row.PayloadJson),
		ErrorMessage:     row.ErrorMessage,
		QueuedAt:         formatTime(row.QueuedAt),
		StartedAt:        formatNullTime(row.StartedAt),
		CompletedAt:      formatNullTime(row.CompletedAt),
		CreatedAt:        formatTime(row.CreatedAt),
		UpdatedAt:        formatTime(row.UpdatedAt),
		QueueKey:         row.QueueKey,
		QueueDisplayName: row.QueueDisplayName,
	}
}

func fromPostgresQueueItemRunGetManyByQueueItemIDJoinedRunsRow(row postgresgen.QueueItemRunGetManyByQueueItemIDJoinedRunsRow) QueueItemRunGetManyByQueueItemIDJoinedRunsRow {
	return QueueItemRunGetManyByQueueItemIDJoinedRunsRow{
		ID:            row.ID,
		QueueItemID:   row.QueueItemID,
		JobID:         row.JobID,
		RunID:         row.RunID,
		CreatedAt:     formatTime(row.CreatedAt),
		RunKey:        row.RunKey,
		RunStatus:     row.RunStatus,
		TriggeredBy:   row.TriggeredBy,
		QueuedAt:      formatTime(row.QueuedAt),
		StartedAt:     formatNullTime(row.StartedAt),
		CompletedAt:   formatNullTime(row.CompletedAt),
		ParentRunID:   row.ParentRunID,
		RerunStepKey:  row.RerunStepKey,
		RunErrorMessage: row.RunErrorMessage,
		JobKey:        row.JobKey,
		JobDisplayName: row.JobDisplayName,
	}
}

func fromPostgresQueueItemStatusCountGetManyByQueueIDRow(row postgresgen.QueueItemStatusCountGetManyByQueueIDRow) QueueItemStatusCountGetManyByQueueIDRow {
	return QueueItemStatusCountGetManyByQueueIDRow{
		Status: row.Status,
		Total:  row.Total,
	}
}

func fromPostgresQueuePartitionGetManyByQueueIDRow(row postgresgen.QueuePartitionGetManyByQueueIDRow) QueuePartitionGetManyByQueueIDRow {
	return QueuePartitionGetManyByQueueIDRow{
		ID:           row.ID,
		QueueID:      row.QueueID,
		QueueItemKey: row.QueueItemKey,
		PartitionKey: row.PartitionKey,
		Status:       row.Status,
		ExternalKey:  row.ExternalKey,
		PayloadJson:  fromRawJSON(row.PayloadJson),
		ErrorMessage: row.ErrorMessage,
		QueuedAt:     formatTime(row.QueuedAt),
		StartedAt:    formatNullTime(row.StartedAt),
		CompletedAt:  formatNullTime(row.CompletedAt),
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}
