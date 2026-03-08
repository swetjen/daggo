package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/swetjen/daggo/db/postgresgen"
)

type PostgresStore struct {
	queries *postgresgen.Queries
}

func (s *PostgresStore) withQueries(queries *postgresgen.Queries) *PostgresStore {
	return &PostgresStore{queries: queries}
}

func (s *PostgresStore) JobGetMany(ctx context.Context, arg JobGetManyParams) ([]Job, error) {
	rows, err := s.queries.JobGetMany(ctx, postgresgen.JobGetManyParams{Limit: int32(arg.Limit), Offset: int32(arg.Offset)})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresJob), nil
}

func (s *PostgresStore) JobCount(ctx context.Context) (int64, error) {
	return s.queries.JobCount(ctx)
}

func (s *PostgresStore) JobGetByKey(ctx context.Context, jobKey string) (Job, error) {
	row, err := s.queries.JobGetByKey(ctx, jobKey)
	if err != nil {
		return Job{}, err
	}
	return fromPostgresJob(row), nil
}

func (s *PostgresStore) JobGetByID(ctx context.Context, id int64) (Job, error) {
	row, err := s.queries.JobGetByID(ctx, id)
	if err != nil {
		return Job{}, err
	}
	return fromPostgresJob(row), nil
}

func (s *PostgresStore) JobDeleteByID(ctx context.Context, id int64) error {
	return s.queries.JobDeleteByID(ctx, id)
}

func (s *PostgresStore) JobUpsert(ctx context.Context, arg JobUpsertParams) (Job, error) {
	row, err := s.queries.JobUpsert(ctx, postgresgen.JobUpsertParams{
		JobKey:            arg.JobKey,
		DisplayName:       arg.DisplayName,
		Description:       arg.Description,
		DefaultParamsJson: []byte(arg.DefaultParamsJson),
	})
	if err != nil {
		return Job{}, err
	}
	return fromPostgresJob(row), nil
}

func (s *PostgresStore) JobNodeDeleteByJobID(ctx context.Context, jobID int64) error {
	return s.queries.JobNodeDeleteByJobID(ctx, jobID)
}

func (s *PostgresStore) JobNodeCreate(ctx context.Context, arg JobNodeCreateParams) (JobNode, error) {
	row, err := s.queries.JobNodeCreate(ctx, postgresgen.JobNodeCreateParams{
		JobID:        arg.JobID,
		StepKey:      arg.StepKey,
		DisplayName:  arg.DisplayName,
		Description:  arg.Description,
		Kind:         arg.Kind,
		MetadataJson: []byte(arg.MetadataJson),
		SortIndex:    arg.SortIndex,
	})
	if err != nil {
		return JobNode{}, err
	}
	return fromPostgresJobNode(row), nil
}

func (s *PostgresStore) JobNodeGetManyByJobID(ctx context.Context, jobID int64) ([]JobNode, error) {
	rows, err := s.queries.JobNodeGetManyByJobID(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresJobNode), nil
}

func (s *PostgresStore) JobEdgeDeleteByJobID(ctx context.Context, jobID int64) error {
	return s.queries.JobEdgeDeleteByJobID(ctx, jobID)
}

func (s *PostgresStore) JobEdgeCreate(ctx context.Context, arg JobEdgeCreateParams) (JobEdge, error) {
	row, err := s.queries.JobEdgeCreate(ctx, postgresgen.JobEdgeCreateParams{
		JobID:       arg.JobID,
		FromStepKey: arg.FromStepKey,
		ToStepKey:   arg.ToStepKey,
	})
	if err != nil {
		return JobEdge{}, err
	}
	return fromPostgresJobEdge(row), nil
}

func (s *PostgresStore) JobEdgeGetManyByJobID(ctx context.Context, jobID int64) ([]JobEdge, error) {
	rows, err := s.queries.JobEdgeGetManyByJobID(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresJobEdge), nil
}

func (s *PostgresStore) RunCreate(ctx context.Context, arg RunCreateParams) (Run, error) {
	row, err := s.queries.RunCreate(ctx, postgresgen.RunCreateParams{
		RunKey:       arg.RunKey,
		JobID:        arg.JobID,
		Status:       arg.Status,
		TriggeredBy:  arg.TriggeredBy,
		ParamsJson:   []byte(arg.ParamsJson),
		QueuedAt:     mustParseStoredTime(arg.QueuedAt),
		StartedAt:    toNullTime(arg.StartedAt),
		CompletedAt:  toNullTime(arg.CompletedAt),
		ParentRunID:  arg.ParentRunID,
		RerunStepKey: arg.RerunStepKey,
		ErrorMessage: arg.ErrorMessage,
	})
	if err != nil {
		return Run{}, err
	}
	return fromPostgresRun(row), nil
}

func (s *PostgresStore) RunGetByID(ctx context.Context, id int64) (Run, error) {
	row, err := s.queries.RunGetByID(ctx, id)
	if err != nil {
		return Run{}, err
	}
	return fromPostgresRun(row), nil
}

func (s *PostgresStore) RunGetManyByJobID(ctx context.Context, arg RunGetManyByJobIDParams) ([]Run, error) {
	rows, err := s.queries.RunGetManyByJobID(ctx, postgresgen.RunGetManyByJobIDParams{
		JobID:  arg.JobID,
		Limit:  int32(arg.Limit),
		Offset: int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresRun), nil
}

func (s *PostgresStore) RunCountByJobID(ctx context.Context, jobID int64) (int64, error) {
	return s.queries.RunCountByJobID(ctx, jobID)
}

func (s *PostgresStore) RunGetMany(ctx context.Context, arg RunGetManyParams) ([]Run, error) {
	rows, err := s.queries.RunGetMany(ctx, postgresgen.RunGetManyParams{Limit: int32(arg.Limit), Offset: int32(arg.Offset)})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresRun), nil
}

func (s *PostgresStore) RunCount(ctx context.Context) (int64, error) {
	return s.queries.RunCount(ctx)
}

func (s *PostgresStore) RunGetManyJoinedJobs(ctx context.Context, arg RunGetManyJoinedJobsParams) ([]RunGetManyJoinedJobsRow, error) {
	rows, err := s.queries.RunGetManyJoinedJobs(ctx, postgresgen.RunGetManyJoinedJobsParams{Limit: int32(arg.Limit), Offset: int32(arg.Offset)})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresRunGetManyJoinedJobsRow), nil
}

func (s *PostgresStore) RunGetManyByJobIDJoinedJobs(ctx context.Context, arg RunGetManyByJobIDJoinedJobsParams) ([]RunGetManyByJobIDJoinedJobsRow, error) {
	rows, err := s.queries.RunGetManyByJobIDJoinedJobs(ctx, postgresgen.RunGetManyByJobIDJoinedJobsParams{
		JobID:  arg.JobID,
		Limit:  int32(arg.Limit),
		Offset: int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresRunGetManyByJobIDJoinedJobsRow), nil
}

func (s *PostgresStore) RunGetByIDJoinedJobs(ctx context.Context, id int64) (RunGetByIDJoinedJobsRow, error) {
	row, err := s.queries.RunGetByIDJoinedJobs(ctx, id)
	if err != nil {
		return RunGetByIDJoinedJobsRow{}, err
	}
	return fromPostgresRunGetByIDJoinedJobsRow(row), nil
}

func (s *PostgresStore) RunUpdateForStart(ctx context.Context, arg RunUpdateForStartParams) (Run, error) {
	row, err := s.queries.RunUpdateForStart(ctx, postgresgen.RunUpdateForStartParams{
		Status:    arg.Status,
		StartedAt: toNullTime(arg.StartedAt),
		ID:        arg.ID,
	})
	if err != nil {
		return Run{}, err
	}
	return fromPostgresRun(row), nil
}

func (s *PostgresStore) RunUpdateForComplete(ctx context.Context, arg RunUpdateForCompleteParams) (Run, error) {
	row, err := s.queries.RunUpdateForComplete(ctx, postgresgen.RunUpdateForCompleteParams{
		Status:       arg.Status,
		CompletedAt:  toNullTime(arg.CompletedAt),
		ErrorMessage: arg.ErrorMessage,
		ID:           arg.ID,
	})
	if err != nil {
		return Run{}, err
	}
	return fromPostgresRun(row), nil
}

func (s *PostgresStore) RunStepCreate(ctx context.Context, arg RunStepCreateParams) (RunStep, error) {
	row, err := s.queries.RunStepCreate(ctx, postgresgen.RunStepCreateParams{
		RunID:        arg.RunID,
		JobNodeID:    arg.JobNodeID,
		StepKey:      arg.StepKey,
		Status:       arg.Status,
		Attempt:      arg.Attempt,
		StartedAt:    toNullTime(arg.StartedAt),
		CompletedAt:  toNullTime(arg.CompletedAt),
		DurationMs:   arg.DurationMs,
		OutputJson:   []byte(arg.OutputJson),
		ErrorMessage: arg.ErrorMessage,
		LogExcerpt:   arg.LogExcerpt,
	})
	if err != nil {
		return RunStep{}, err
	}
	return fromPostgresRunStep(row), nil
}

func (s *PostgresStore) RunStepGetManyByRunID(ctx context.Context, runID int64) ([]RunStep, error) {
	rows, err := s.queries.RunStepGetManyByRunID(ctx, runID)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresRunStep), nil
}

func (s *PostgresStore) RunStepGetByRunIDAndStepKey(ctx context.Context, arg RunStepGetByRunIDAndStepKeyParams) (RunStep, error) {
	row, err := s.queries.RunStepGetByRunIDAndStepKey(ctx, postgresgen.RunStepGetByRunIDAndStepKeyParams{
		RunID:   arg.RunID,
		StepKey: arg.StepKey,
	})
	if err != nil {
		return RunStep{}, err
	}
	return fromPostgresRunStep(row), nil
}

func (s *PostgresStore) RunStepUpdateForStart(ctx context.Context, arg RunStepUpdateForStartParams) (RunStep, error) {
	row, err := s.queries.RunStepUpdateForStart(ctx, postgresgen.RunStepUpdateForStartParams{
		Status:    arg.Status,
		StartedAt: toNullTime(arg.StartedAt),
		RunID:     arg.RunID,
		StepKey:   arg.StepKey,
	})
	if err != nil {
		return RunStep{}, err
	}
	return fromPostgresRunStep(row), nil
}

func (s *PostgresStore) RunStepUpdateForComplete(ctx context.Context, arg RunStepUpdateForCompleteParams) (RunStep, error) {
	row, err := s.queries.RunStepUpdateForComplete(ctx, postgresgen.RunStepUpdateForCompleteParams{
		Status:       arg.Status,
		CompletedAt:  toNullTime(arg.CompletedAt),
		DurationMs:   arg.DurationMs,
		OutputJson:   []byte(arg.OutputJson),
		ErrorMessage: arg.ErrorMessage,
		LogExcerpt:   arg.LogExcerpt,
		RunID:        arg.RunID,
		StepKey:      arg.StepKey,
	})
	if err != nil {
		return RunStep{}, err
	}
	return fromPostgresRunStep(row), nil
}

func (s *PostgresStore) RunEventCreate(ctx context.Context, arg RunEventCreateParams) (RunEvent, error) {
	row, err := s.queries.RunEventCreate(ctx, postgresgen.RunEventCreateParams{
		RunID:         arg.RunID,
		StepKey:       arg.StepKey,
		EventType:     arg.EventType,
		Level:         arg.Level,
		Message:       arg.Message,
		EventDataJson: []byte(arg.EventDataJson),
	})
	if err != nil {
		return RunEvent{}, err
	}
	return fromPostgresRunEvent(row), nil
}

func (s *PostgresStore) RunEventGetManyByRunID(ctx context.Context, arg RunEventGetManyByRunIDParams) ([]RunEvent, error) {
	rows, err := s.queries.RunEventGetManyByRunID(ctx, postgresgen.RunEventGetManyByRunIDParams{
		RunID:  arg.RunID,
		Limit:  int32(arg.Limit),
		Offset: int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresRunEvent), nil
}

func (s *PostgresStore) RunEventCountByRunID(ctx context.Context, runID int64) (int64, error) {
	return s.queries.RunEventCountByRunID(ctx, runID)
}

func (s *PostgresStore) BackfillGetMany(ctx context.Context, arg BackfillGetManyParams) ([]Backfill, error) {
	rows, err := s.queries.BackfillGetMany(ctx, postgresgen.BackfillGetManyParams{
		Limit:  int32(arg.Limit),
		Offset: int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresBackfill), nil
}

func (s *PostgresStore) BackfillCount(ctx context.Context) (int64, error) {
	return s.queries.BackfillCount(ctx)
}

func (s *PostgresStore) BackfillGetManyByJobID(ctx context.Context, arg BackfillGetManyByJobIDParams) ([]Backfill, error) {
	rows, err := s.queries.BackfillGetManyByJobID(ctx, postgresgen.BackfillGetManyByJobIDParams{
		JobID:  arg.JobID,
		Limit:  int32(arg.Limit),
		Offset: int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresBackfill), nil
}

func (s *PostgresStore) BackfillCountByJobID(ctx context.Context, jobID int64) (int64, error) {
	return s.queries.BackfillCountByJobID(ctx, jobID)
}

func (s *PostgresStore) BackfillGetByKey(ctx context.Context, backfillKey string) (Backfill, error) {
	row, err := s.queries.BackfillGetByKey(ctx, backfillKey)
	if err != nil {
		return Backfill{}, err
	}
	return fromPostgresBackfill(row), nil
}

func (s *PostgresStore) BackfillCreate(ctx context.Context, arg BackfillCreateParams) (Backfill, error) {
	row, err := s.queries.BackfillCreate(ctx, postgresgen.BackfillCreateParams{
		BackfillKey:             arg.BackfillKey,
		JobID:                   arg.JobID,
		PartitionDefinitionID:   arg.PartitionDefinitionID,
		Status:                  arg.Status,
		SelectionMode:           arg.SelectionMode,
		SelectionJson:           []byte(arg.SelectionJson),
		TriggeredBy:             arg.TriggeredBy,
		PolicyMode:              arg.PolicyMode,
		MaxPartitionsPerRun:     arg.MaxPartitionsPerRun,
		RequestedPartitionCount: arg.RequestedPartitionCount,
		RequestedRunCount:       arg.RequestedRunCount,
		CompletedPartitionCount: arg.CompletedPartitionCount,
		FailedPartitionCount:    arg.FailedPartitionCount,
		ErrorMessage:            arg.ErrorMessage,
		StartedAt:               toNullTime(arg.StartedAt),
		CompletedAt:             toNullTime(arg.CompletedAt),
	})
	if err != nil {
		return Backfill{}, err
	}
	return fromPostgresBackfill(row), nil
}

func (s *PostgresStore) BackfillPartitionGetManyByBackfillID(ctx context.Context, arg BackfillPartitionGetManyByBackfillIDParams) ([]BackfillPartition, error) {
	rows, err := s.queries.BackfillPartitionGetManyByBackfillID(ctx, postgresgen.BackfillPartitionGetManyByBackfillIDParams{
		BackfillID: arg.BackfillID,
		Limit:      int32(arg.Limit),
		Offset:     int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresBackfillPartition), nil
}

func (s *PostgresStore) BackfillPartitionCountByBackfillID(ctx context.Context, backfillID int64) (int64, error) {
	return s.queries.BackfillPartitionCountByBackfillID(ctx, backfillID)
}

func (s *PostgresStore) BackfillPartitionCountByBackfillIDAndStatus(ctx context.Context, arg BackfillPartitionCountByBackfillIDAndStatusParams) (int64, error) {
	return s.queries.BackfillPartitionCountByBackfillIDAndStatus(ctx, postgresgen.BackfillPartitionCountByBackfillIDAndStatusParams{
		BackfillID: arg.BackfillID,
		Status:     arg.Status,
	})
}

func (s *PostgresStore) BackfillPartitionUpsert(ctx context.Context, arg BackfillPartitionUpsertParams) (BackfillPartition, error) {
	row, err := s.queries.BackfillPartitionUpsert(ctx, postgresgen.BackfillPartitionUpsertParams{
		BackfillID:   arg.BackfillID,
		PartitionKey: arg.PartitionKey,
		Status:       arg.Status,
		RunID:        arg.RunID,
		ErrorMessage: arg.ErrorMessage,
	})
	if err != nil {
		return BackfillPartition{}, err
	}
	return fromPostgresBackfillPartition(row), nil
}

func (s *PostgresStore) BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey(ctx context.Context, arg BackfillPartitionUpdateStatusByBackfillIDAndPartitionKeyParams) (BackfillPartition, error) {
	row, err := s.queries.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey(ctx, postgresgen.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKeyParams{
		Status:       arg.Status,
		RunID:        arg.RunID,
		ErrorMessage: arg.ErrorMessage,
		BackfillID:   arg.BackfillID,
		PartitionKey: arg.PartitionKey,
	})
	if err != nil {
		return BackfillPartition{}, err
	}
	return fromPostgresBackfillPartition(row), nil
}

func (s *PostgresStore) RunPartitionTargetGetManyByBackfillKey(ctx context.Context, arg RunPartitionTargetGetManyByBackfillKeyParams) ([]RunPartitionTarget, error) {
	rows, err := s.queries.RunPartitionTargetGetManyByBackfillKey(ctx, postgresgen.RunPartitionTargetGetManyByBackfillKeyParams{
		BackfillKey: arg.BackfillKey,
		Limit:       int32(arg.Limit),
		Offset:      int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresRunPartitionTarget), nil
}

func (s *PostgresStore) RunPartitionTargetUpsert(ctx context.Context, arg RunPartitionTargetUpsertParams) (RunPartitionTarget, error) {
	row, err := s.queries.RunPartitionTargetUpsert(ctx, postgresgen.RunPartitionTargetUpsertParams{
		RunID:                 arg.RunID,
		PartitionDefinitionID: arg.PartitionDefinitionID,
		SelectionMode:         arg.SelectionMode,
		PartitionKey:          arg.PartitionKey,
		RangeStartKey:         arg.RangeStartKey,
		RangeEndKey:           arg.RangeEndKey,
		PartitionSubsetJson:   []byte(arg.PartitionSubsetJson),
		TagsJson:              []byte(arg.TagsJson),
		BackfillKey:           arg.BackfillKey,
	})
	if err != nil {
		return RunPartitionTarget{}, err
	}
	return fromPostgresRunPartitionTarget(row), nil
}

func (s *PostgresStore) RunSystemTagUpsert(ctx context.Context, arg RunSystemTagUpsertParams) (RunSystemTag, error) {
	row, err := s.queries.RunSystemTagUpsert(ctx, postgresgen.RunSystemTagUpsertParams{
		RunID:    arg.RunID,
		TagKey:   arg.TagKey,
		TagValue: arg.TagValue,
	})
	if err != nil {
		return RunSystemTag{}, err
	}
	return fromPostgresRunSystemTag(row), nil
}

func (s *PostgresStore) PartitionDefinitionGetByJobIDAndTarget(ctx context.Context, arg PartitionDefinitionGetByJobIDAndTargetParams) (PartitionDefinition, error) {
	row, err := s.queries.PartitionDefinitionGetByJobIDAndTarget(ctx, postgresgen.PartitionDefinitionGetByJobIDAndTargetParams{
		JobID:      arg.JobID,
		TargetKind: arg.TargetKind,
		TargetKey:  arg.TargetKey,
	})
	if err != nil {
		return PartitionDefinition{}, err
	}
	return fromPostgresPartitionDefinition(row), nil
}

func (s *PostgresStore) PartitionKeyGetManyByDefinitionID(ctx context.Context, arg PartitionKeyGetManyByDefinitionIDParams) ([]PartitionKey, error) {
	rows, err := s.queries.PartitionKeyGetManyByDefinitionID(ctx, postgresgen.PartitionKeyGetManyByDefinitionIDParams{
		PartitionDefinitionID: arg.PartitionDefinitionID,
		Limit:                 int32(arg.Limit),
		Offset:                int32(arg.Offset),
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresPartitionKey), nil
}

func (s *PostgresStore) PartitionKeyCountByDefinitionID(ctx context.Context, partitionDefinitionID int64) (int64, error) {
	return s.queries.PartitionKeyCountByDefinitionID(ctx, partitionDefinitionID)
}

func (s *PostgresStore) SchedulerHeartbeatUpsert(ctx context.Context, arg SchedulerHeartbeatUpsertParams) (SchedulerHeartbeat, error) {
	row, err := s.queries.SchedulerHeartbeatUpsert(ctx, postgresgen.SchedulerHeartbeatUpsertParams{
		SchedulerKey:        arg.SchedulerKey,
		LastHeartbeatAt:     mustParseStoredTime(arg.LastHeartbeatAt),
		LastTickStartedAt:   toNullTime(arg.LastTickStartedAt),
		LastTickCompletedAt: toNullTime(arg.LastTickCompletedAt),
		LastError:           arg.LastError,
	})
	if err != nil {
		return SchedulerHeartbeat{}, err
	}
	return fromPostgresSchedulerHeartbeat(row), nil
}

func (s *PostgresStore) SchedulerScheduleStateGetByJobKeyScheduleKey(ctx context.Context, arg SchedulerScheduleStateGetByJobKeyScheduleKeyParams) (SchedulerScheduleState, error) {
	row, err := s.queries.SchedulerScheduleStateGetByJobKeyScheduleKey(ctx, postgresgen.SchedulerScheduleStateGetByJobKeyScheduleKeyParams{
		JobKey:      arg.JobKey,
		ScheduleKey: arg.ScheduleKey,
	})
	if err != nil {
		return SchedulerScheduleState{}, err
	}
	return fromPostgresSchedulerScheduleState(row), nil
}

func (s *PostgresStore) SchedulerScheduleStateUpsert(ctx context.Context, arg SchedulerScheduleStateUpsertParams) (SchedulerScheduleState, error) {
	row, err := s.queries.SchedulerScheduleStateUpsert(ctx, postgresgen.SchedulerScheduleStateUpsertParams{
		JobKey:         arg.JobKey,
		ScheduleKey:    arg.ScheduleKey,
		LastCheckedAt:  toNullTime(arg.LastCheckedAt),
		LastEnqueuedAt: toNullTime(arg.LastEnqueuedAt),
		NextRunAt:      toNullTime(arg.NextRunAt),
	})
	if err != nil {
		return SchedulerScheduleState{}, err
	}
	return fromPostgresSchedulerScheduleState(row), nil
}

func (s *PostgresStore) SchedulerScheduleStateGetMany(ctx context.Context) ([]SchedulerScheduleState, error) {
	rows, err := s.queries.SchedulerScheduleStateGetMany(ctx)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresSchedulerScheduleState), nil
}

func (s *PostgresStore) SchedulerScheduleStateDeleteByJobKeyScheduleKey(ctx context.Context, arg SchedulerScheduleStateDeleteByJobKeyScheduleKeyParams) error {
	return s.queries.SchedulerScheduleStateDeleteByJobKeyScheduleKey(ctx, postgresgen.SchedulerScheduleStateDeleteByJobKeyScheduleKeyParams{
		JobKey:      arg.JobKey,
		ScheduleKey: arg.ScheduleKey,
	})
}

func (s *PostgresStore) SchedulerScheduleRunsCreateIfAbsent(ctx context.Context, arg SchedulerScheduleRunsCreateIfAbsentParams) ([]SchedulerScheduleRun, error) {
	rows, err := s.queries.SchedulerScheduleRunsCreateIfAbsent(ctx, postgresgen.SchedulerScheduleRunsCreateIfAbsentParams{
		JobKey:       arg.JobKey,
		ScheduleKey:  arg.ScheduleKey,
		ScheduledFor: mustParseStoredTime(arg.ScheduledFor),
		RunKey:       arg.RunKey,
		TriggeredBy:  arg.TriggeredBy,
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresSchedulerScheduleRun), nil
}

func (s *PostgresStore) SchedulerScheduleRunUpdateByID(ctx context.Context, arg SchedulerScheduleRunUpdateByIDParams) (SchedulerScheduleRun, error) {
	row, err := s.queries.SchedulerScheduleRunUpdateByID(ctx, postgresgen.SchedulerScheduleRunUpdateByIDParams{
		RunKey:      arg.RunKey,
		TriggeredBy: arg.TriggeredBy,
		ID:          arg.ID,
	})
	if err != nil {
		return SchedulerScheduleRun{}, err
	}
	return fromPostgresSchedulerScheduleRun(row), nil
}

func (s *PostgresStore) SchedulerScheduleRunDeleteByID(ctx context.Context, id int64) error {
	return s.queries.SchedulerScheduleRunDeleteByID(ctx, id)
}

func (s *PostgresStore) SchedulerScheduleRunGetDistinctMany(ctx context.Context) ([]SchedulerScheduleRunGetDistinctManyRow, error) {
	rows, err := s.queries.SchedulerScheduleRunGetDistinctMany(ctx)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromPostgresSchedulerScheduleRunGetDistinctManyRow), nil
}

func (s *PostgresStore) SchedulerScheduleRunsDeleteByJobKeyScheduleKey(ctx context.Context, arg SchedulerScheduleRunsDeleteByJobKeyScheduleKeyParams) error {
	return s.queries.SchedulerScheduleRunsDeleteByJobKeyScheduleKey(ctx, postgresgen.SchedulerScheduleRunsDeleteByJobKeyScheduleKeyParams{
		JobKey:      arg.JobKey,
		ScheduleKey: arg.ScheduleKey,
	})
}

func mapSlice[A any, B any](rows []A, mapper func(A) B) []B {
	out := make([]B, 0, len(rows))
	for _, row := range rows {
		out = append(out, mapper(row))
	}
	return out
}

func fromPostgresJob(row postgresgen.Job) Job {
	return Job{
		ID:                row.ID,
		JobKey:            row.JobKey,
		DisplayName:       row.DisplayName,
		Description:       row.Description,
		DefaultParamsJson: fromRawJSON(row.DefaultParamsJson),
		CreatedAt:         formatTime(row.CreatedAt),
		UpdatedAt:         formatTime(row.UpdatedAt),
	}
}

func fromPostgresJobNode(row postgresgen.JobNode) JobNode {
	return JobNode{
		ID:           row.ID,
		JobID:        row.JobID,
		StepKey:      row.StepKey,
		DisplayName:  row.DisplayName,
		Description:  row.Description,
		Kind:         row.Kind,
		MetadataJson: fromRawJSON(row.MetadataJson),
		SortIndex:    row.SortIndex,
		CreatedAt:    formatTime(row.CreatedAt),
	}
}

func fromPostgresJobEdge(row postgresgen.JobEdge) JobEdge {
	return JobEdge{
		ID:          row.ID,
		JobID:       row.JobID,
		FromStepKey: row.FromStepKey,
		ToStepKey:   row.ToStepKey,
		CreatedAt:   formatTime(row.CreatedAt),
	}
}

func fromPostgresRun(row postgresgen.Run) Run {
	return Run{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   fromRawJSON(row.ParamsJson),
		QueuedAt:     formatTime(row.QueuedAt),
		StartedAt:    formatNullTime(row.StartedAt),
		CompletedAt:  formatNullTime(row.CompletedAt),
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}

func fromPostgresRunGetManyJoinedJobsRow(row postgresgen.RunGetManyJoinedJobsRow) RunGetManyJoinedJobsRow {
	return RunGetManyJoinedJobsRow{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   fromRawJSON(row.ParamsJson),
		QueuedAt:     formatTime(row.QueuedAt),
		StartedAt:    formatNullTime(row.StartedAt),
		CompletedAt:  formatNullTime(row.CompletedAt),
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}

func fromPostgresRunGetManyByJobIDJoinedJobsRow(row postgresgen.RunGetManyByJobIDJoinedJobsRow) RunGetManyByJobIDJoinedJobsRow {
	return RunGetManyByJobIDJoinedJobsRow{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   fromRawJSON(row.ParamsJson),
		QueuedAt:     formatTime(row.QueuedAt),
		StartedAt:    formatNullTime(row.StartedAt),
		CompletedAt:  formatNullTime(row.CompletedAt),
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}

func fromPostgresRunGetByIDJoinedJobsRow(row postgresgen.RunGetByIDJoinedJobsRow) RunGetByIDJoinedJobsRow {
	return RunGetByIDJoinedJobsRow{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   fromRawJSON(row.ParamsJson),
		QueuedAt:     formatTime(row.QueuedAt),
		StartedAt:    formatNullTime(row.StartedAt),
		CompletedAt:  formatNullTime(row.CompletedAt),
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}

func fromPostgresRunStep(row postgresgen.RunStep) RunStep {
	return RunStep{
		ID:           row.ID,
		RunID:        row.RunID,
		JobNodeID:    row.JobNodeID,
		StepKey:      row.StepKey,
		Status:       row.Status,
		Attempt:      row.Attempt,
		StartedAt:    formatNullTime(row.StartedAt),
		CompletedAt:  formatNullTime(row.CompletedAt),
		DurationMs:   row.DurationMs,
		OutputJson:   fromRawJSON(row.OutputJson),
		ErrorMessage: row.ErrorMessage,
		LogExcerpt:   row.LogExcerpt,
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}

func fromPostgresRunEvent(row postgresgen.RunEvent) RunEvent {
	return RunEvent{
		ID:            row.ID,
		RunID:         row.RunID,
		StepKey:       row.StepKey,
		EventType:     row.EventType,
		Level:         row.Level,
		Message:       row.Message,
		EventDataJson: fromRawJSON(row.EventDataJson),
		CreatedAt:     formatTime(row.CreatedAt),
	}
}

func fromPostgresBackfill(row postgresgen.Backfill) Backfill {
	return Backfill{
		ID:                      row.ID,
		BackfillKey:             row.BackfillKey,
		JobID:                   row.JobID,
		PartitionDefinitionID:   row.PartitionDefinitionID,
		Status:                  row.Status,
		SelectionMode:           row.SelectionMode,
		SelectionJson:           fromRawJSON(row.SelectionJson),
		TriggeredBy:             row.TriggeredBy,
		PolicyMode:              row.PolicyMode,
		MaxPartitionsPerRun:     row.MaxPartitionsPerRun,
		RequestedPartitionCount: row.RequestedPartitionCount,
		RequestedRunCount:       row.RequestedRunCount,
		CompletedPartitionCount: row.CompletedPartitionCount,
		FailedPartitionCount:    row.FailedPartitionCount,
		ErrorMessage:            row.ErrorMessage,
		StartedAt:               formatNullTime(row.StartedAt),
		CompletedAt:             formatNullTime(row.CompletedAt),
		CreatedAt:               formatTime(row.CreatedAt),
		UpdatedAt:               formatTime(row.UpdatedAt),
	}
}

func fromPostgresBackfillPartition(row postgresgen.BackfillPartition) BackfillPartition {
	return BackfillPartition{
		ID:           row.ID,
		BackfillID:   row.BackfillID,
		PartitionKey: row.PartitionKey,
		Status:       row.Status,
		RunID:        row.RunID,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}

func fromPostgresPartitionDefinition(row postgresgen.PartitionDefinition) PartitionDefinition {
	return PartitionDefinition{
		ID:             row.ID,
		JobID:          row.JobID,
		TargetKind:     row.TargetKind,
		TargetKey:      row.TargetKey,
		DefinitionKind: row.DefinitionKind,
		DefinitionJson: fromRawJSON(row.DefinitionJson),
		Enabled:        boolToInt64(row.Enabled),
		CreatedAt:      formatTime(row.CreatedAt),
		UpdatedAt:      formatTime(row.UpdatedAt),
	}
}

func fromPostgresPartitionKey(row postgresgen.PartitionKey) PartitionKey {
	return PartitionKey{
		ID:                    row.ID,
		PartitionDefinitionID: row.PartitionDefinitionID,
		PartitionKey:          row.PartitionKey,
		SortIndex:             row.SortIndex,
		IsActive:              boolToInt64(row.IsActive),
		CreatedAt:             formatTime(row.CreatedAt),
		UpdatedAt:             formatTime(row.UpdatedAt),
	}
}

func fromPostgresRunPartitionTarget(row postgresgen.RunPartitionTarget) RunPartitionTarget {
	return RunPartitionTarget{
		RunID:                 row.RunID,
		PartitionDefinitionID: row.PartitionDefinitionID,
		SelectionMode:         row.SelectionMode,
		PartitionKey:          row.PartitionKey,
		RangeStartKey:         row.RangeStartKey,
		RangeEndKey:           row.RangeEndKey,
		PartitionSubsetJson:   fromRawJSON(row.PartitionSubsetJson),
		TagsJson:              fromRawJSON(row.TagsJson),
		BackfillKey:           row.BackfillKey,
		CreatedAt:             formatTime(row.CreatedAt),
		UpdatedAt:             formatTime(row.UpdatedAt),
	}
}

func fromPostgresRunSystemTag(row postgresgen.RunSystemTag) RunSystemTag {
	return RunSystemTag{
		ID:        row.ID,
		RunID:     row.RunID,
		TagKey:    row.TagKey,
		TagValue:  row.TagValue,
		CreatedAt: formatTime(row.CreatedAt),
	}
}

func fromPostgresSchedulerHeartbeat(row postgresgen.SchedulerHeartbeat) SchedulerHeartbeat {
	return SchedulerHeartbeat{
		SchedulerKey:        row.SchedulerKey,
		LastHeartbeatAt:     formatTime(row.LastHeartbeatAt),
		LastTickStartedAt:   formatNullTime(row.LastTickStartedAt),
		LastTickCompletedAt: formatNullTime(row.LastTickCompletedAt),
		LastError:           row.LastError,
		UpdatedAt:           formatTime(row.UpdatedAt),
	}
}

func fromPostgresSchedulerScheduleState(row postgresgen.SchedulerScheduleState) SchedulerScheduleState {
	return SchedulerScheduleState{
		JobKey:         row.JobKey,
		ScheduleKey:    row.ScheduleKey,
		LastCheckedAt:  formatNullTime(row.LastCheckedAt),
		LastEnqueuedAt: formatNullTime(row.LastEnqueuedAt),
		NextRunAt:      formatNullTime(row.NextRunAt),
		UpdatedAt:      formatTime(row.UpdatedAt),
	}
}

func fromPostgresSchedulerScheduleRun(row postgresgen.SchedulerScheduleRun) SchedulerScheduleRun {
	return SchedulerScheduleRun{
		ID:           row.ID,
		JobKey:       row.JobKey,
		ScheduleKey:  row.ScheduleKey,
		ScheduledFor: formatTime(row.ScheduledFor),
		RunKey:       row.RunKey,
		TriggeredBy:  row.TriggeredBy,
		CreatedAt:    formatTime(row.CreatedAt),
		UpdatedAt:    formatTime(row.UpdatedAt),
	}
}

func fromPostgresSchedulerScheduleRunGetDistinctManyRow(row postgresgen.SchedulerScheduleRunGetDistinctManyRow) SchedulerScheduleRunGetDistinctManyRow {
	return SchedulerScheduleRunGetDistinctManyRow{
		JobKey:      row.JobKey,
		ScheduleKey: row.ScheduleKey,
	}
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func formatNullTime(value sql.NullTime) string {
	if !value.Valid {
		return ""
	}
	return formatTime(value.Time)
}

func toNullTime(value string) sql.NullTime {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return sql.NullTime{}
	}
	return sql.NullTime{
		Time:  mustParseStoredTime(trimmed),
		Valid: true,
	}
}

func mustParseStoredTime(value string) time.Time {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return parsed.UTC()
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return parsed.UTC()
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", trimmed); err == nil {
		return parsed.UTC()
	}
	return time.Time{}
}

func fromRawJSON(value json.RawMessage) string {
	if len(value) == 0 {
		return ""
	}
	return string(value)
}

func boolToInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
