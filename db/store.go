package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/swetjen/daggo/db/postgresgen"
)

type Store interface {
	JobGetMany(context.Context, JobGetManyParams) ([]Job, error)
	JobCount(context.Context) (int64, error)
	JobGetByKey(context.Context, string) (Job, error)
	JobGetByID(context.Context, int64) (Job, error)
	JobDeleteByID(context.Context, int64) error
	JobUpsert(context.Context, JobUpsertParams) (Job, error)
	JobNodeDeleteByJobID(context.Context, int64) error
	JobNodeCreate(context.Context, JobNodeCreateParams) (JobNode, error)
	JobNodeGetManyByJobID(context.Context, int64) ([]JobNode, error)
	JobEdgeDeleteByJobID(context.Context, int64) error
	JobEdgeCreate(context.Context, JobEdgeCreateParams) (JobEdge, error)
	JobEdgeGetManyByJobID(context.Context, int64) ([]JobEdge, error)

	QueueGetMany(context.Context, QueueGetManyParams) ([]Queue, error)
	QueueCount(context.Context) (int64, error)
	QueueGetByKey(context.Context, string) (Queue, error)
	QueueUpsert(context.Context, QueueUpsertParams) (Queue, error)
	QueueJobDeleteByQueueID(context.Context, int64) error
	QueueJobCreate(context.Context, QueueJobCreateParams) (QueueJob, error)
	QueueItemCreate(context.Context, QueueItemCreateParams) (QueueItem, error)
	QueueItemGetByIDJoinedQueues(context.Context, int64) (QueueItemGetByIDJoinedQueuesRow, error)
	QueueItemGetManyByQueueID(context.Context, QueueItemGetManyByQueueIDParams) ([]QueueItem, error)
	QueueItemCountByQueueID(context.Context, int64) (int64, error)
	QueueItemUpdateLifecycleByID(context.Context, QueueItemUpdateLifecycleByIDParams) (QueueItem, error)
	QueueItemStatusCountGetManyByQueueID(context.Context, int64) ([]QueueItemStatusCountGetManyByQueueIDRow, error)
	QueuePartitionGetManyByQueueID(context.Context, QueuePartitionGetManyByQueueIDParams) ([]QueuePartitionGetManyByQueueIDRow, error)
	QueuePartitionCountByQueueID(context.Context, int64) (int64, error)
	QueueItemRunCreate(context.Context, QueueItemRunCreateParams) (QueueItemRun, error)
	QueueItemRunGetByRunID(context.Context, int64) (QueueItemRun, error)
	QueueItemRunGetManyByQueueItemIDJoinedRuns(context.Context, int64) ([]QueueItemRunGetManyByQueueItemIDJoinedRunsRow, error)
	QueueItemStepMetadataUpsert(context.Context, QueueItemStepMetadataUpsertParams) (QueueItemStepMetadatum, error)
	QueueItemStepMetadataGetManyByQueueItemID(context.Context, int64) ([]QueueItemStepMetadatum, error)

	RunCreate(context.Context, RunCreateParams) (Run, error)
	RunGetByID(context.Context, int64) (Run, error)
	RunGetManyByJobID(context.Context, RunGetManyByJobIDParams) ([]Run, error)
	RunCountByJobID(context.Context, int64) (int64, error)
	RunGetMany(context.Context, RunGetManyParams) ([]Run, error)
	RunCount(context.Context) (int64, error)
	RunGetManyJoinedJobs(context.Context, RunGetManyJoinedJobsParams) ([]RunGetManyJoinedJobsRow, error)
	RunGetManyByJobIDJoinedJobs(context.Context, RunGetManyByJobIDJoinedJobsParams) ([]RunGetManyByJobIDJoinedJobsRow, error)
	RunGetByIDJoinedJobs(context.Context, int64) (RunGetByIDJoinedJobsRow, error)
	RunUpdateForStart(context.Context, RunUpdateForStartParams) (Run, error)
	RunUpdateForComplete(context.Context, RunUpdateForCompleteParams) (Run, error)
	RunStepCreate(context.Context, RunStepCreateParams) (RunStep, error)
	RunStepGetManyByRunID(context.Context, int64) ([]RunStep, error)
	RunStepGetByRunIDAndStepKey(context.Context, RunStepGetByRunIDAndStepKeyParams) (RunStep, error)
	RunStepUpdateForStart(context.Context, RunStepUpdateForStartParams) (RunStep, error)
	RunStepUpdateForComplete(context.Context, RunStepUpdateForCompleteParams) (RunStep, error)
	RunEventCreate(context.Context, RunEventCreateParams) (RunEvent, error)
	RunEventGetManyByRunID(context.Context, RunEventGetManyByRunIDParams) ([]RunEvent, error)
	RunEventCountByRunID(context.Context, int64) (int64, error)

	SchedulerHeartbeatUpsert(context.Context, SchedulerHeartbeatUpsertParams) (SchedulerHeartbeat, error)
	SchedulerScheduleStateGetByJobKeyScheduleKey(context.Context, SchedulerScheduleStateGetByJobKeyScheduleKeyParams) (SchedulerScheduleState, error)
	SchedulerScheduleStateUpsert(context.Context, SchedulerScheduleStateUpsertParams) (SchedulerScheduleState, error)
	SchedulerScheduleStateGetMany(context.Context) ([]SchedulerScheduleState, error)
	SchedulerScheduleStateDeleteByJobKeyScheduleKey(context.Context, SchedulerScheduleStateDeleteByJobKeyScheduleKeyParams) error
	SchedulerScheduleRunsCreateIfAbsent(context.Context, SchedulerScheduleRunsCreateIfAbsentParams) ([]SchedulerScheduleRun, error)
	SchedulerScheduleRunUpdateByID(context.Context, SchedulerScheduleRunUpdateByIDParams) (SchedulerScheduleRun, error)
	SchedulerScheduleRunDeleteByID(context.Context, int64) error
	SchedulerScheduleRunGetDistinctMany(context.Context) ([]SchedulerScheduleRunGetDistinctManyRow, error)
	SchedulerScheduleRunsDeleteByJobKeyScheduleKey(context.Context, SchedulerScheduleRunsDeleteByJobKeyScheduleKeyParams) error
}

func WithTx(store Store, tx *sql.Tx) Store {
	switch typed := store.(type) {
	case *Queries:
		return typed.WithTx(tx)
	case *PostgresStore:
		return typed.withQueries(typed.queries.WithTx(tx))
	default:
		panic(fmt.Sprintf("db.WithTx: unsupported store type %T", store))
	}
}

func NewPostgresStore(dbtx postgresgen.DBTX) *PostgresStore {
	return &PostgresStore{queries: postgresgen.New(dbtx)}
}
