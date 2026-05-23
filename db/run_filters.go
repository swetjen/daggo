package db

import "context"

type RunFilteredParams struct {
	JobID       int64
	Status      string
	Search      string
	QuickFilter string
	WindowHours int64
	Sort        string
	Limit       int64
	Offset      int64
}

type RunWindowParams struct {
	JobID       int64
	WindowStart string
	WindowEnd   string
	Limit       int64
	Offset      int64
}

type RunCursorParams struct {
	JobID       int64
	Status      string
	Search      string
	QuickFilter string
	WindowHours int64
	Sort        string
	CursorAt    string
	CursorID    int64
	Limit       int64
}

func (q *Queries) RunFilteredCount(ctx context.Context, arg RunFilteredParams) (int64, error) {
	return q.RunCountFiltered(ctx, RunCountFilteredParams{
		JobID:       arg.JobID,
		Status:      arg.Status,
		Search:      arg.Search,
		QuickFilter: arg.QuickFilter,
		WindowHours: arg.WindowHours,
	})
}

func (q *Queries) RunFilteredGetMany(ctx context.Context, arg RunFilteredParams) ([]RunGetManyJoinedJobsRow, error) {
	switch arg.Sort {
	case "oldest":
		rows, err := q.RunGetManyFilteredJoinedJobsOldest(ctx, RunGetManyFilteredJoinedJobsOldestParams{
			JobID:       arg.JobID,
			Status:      arg.Status,
			Search:      arg.Search,
			QuickFilter: arg.QuickFilter,
			WindowHours: arg.WindowHours,
			Limit:       arg.Limit,
			Offset:      arg.Offset,
		})
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, fromSQLiteFilteredRunsOldestRow), nil
	case "duration_desc":
		rows, err := q.RunGetManyFilteredJoinedJobsDurationDesc(ctx, RunGetManyFilteredJoinedJobsDurationDescParams{
			JobID:       arg.JobID,
			Status:      arg.Status,
			Search:      arg.Search,
			QuickFilter: arg.QuickFilter,
			WindowHours: arg.WindowHours,
			Limit:       arg.Limit,
			Offset:      arg.Offset,
		})
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, fromSQLiteFilteredRunsDurationRow), nil
	default:
		rows, err := q.RunGetManyFilteredJoinedJobs(ctx, RunGetManyFilteredJoinedJobsParams{
			JobID:       arg.JobID,
			Status:      arg.Status,
			Search:      arg.Search,
			QuickFilter: arg.QuickFilter,
			WindowHours: arg.WindowHours,
			Limit:       arg.Limit,
			Offset:      arg.Offset,
		})
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, fromSQLiteFilteredRunsRow), nil
	}
}

func (q *Queries) RunWindowGetMany(ctx context.Context, arg RunWindowParams) ([]RunGetManyJoinedJobsRow, error) {
	rows, err := q.RunGetManyByJobIDWithinWindowJoinedJobs(ctx, RunGetManyByJobIDWithinWindowJoinedJobsParams{
		JobID:       arg.JobID,
		WindowStart: arg.WindowStart,
		WindowEnd:   arg.WindowEnd,
		Limit:       arg.Limit,
		Offset:      arg.Offset,
	})
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, fromSQLiteWindowedRunsRow), nil
}

func (q *Queries) RunCursorGetMany(ctx context.Context, arg RunCursorParams) ([]RunGetManyJoinedJobsRow, error) {
	switch arg.Sort {
	case "oldest":
		rows, err := q.RunGetManyFilteredJoinedJobsCursorOldest(ctx, RunGetManyFilteredJoinedJobsCursorOldestParams{
			JobID:       arg.JobID,
			Status:      arg.Status,
			Search:      arg.Search,
			QuickFilter: arg.QuickFilter,
			WindowHours: arg.WindowHours,
			CursorAt:    arg.CursorAt,
			CursorID:    arg.CursorID,
			Limit:       arg.Limit,
		})
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, fromSQLiteCursorOldestRow), nil
	default:
		rows, err := q.RunGetManyFilteredJoinedJobsCursorNewest(ctx, RunGetManyFilteredJoinedJobsCursorNewestParams{
			JobID:       arg.JobID,
			Status:      arg.Status,
			Search:      arg.Search,
			QuickFilter: arg.QuickFilter,
			WindowHours: arg.WindowHours,
			CursorAt:    arg.CursorAt,
			CursorID:    arg.CursorID,
			Limit:       arg.Limit,
		})
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, fromSQLiteCursorNewestRow), nil
	}
}

func fromSQLiteFilteredRunsRow(row RunGetManyFilteredJoinedJobsRow) RunGetManyJoinedJobsRow {
	return RunGetManyJoinedJobsRow{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   row.ParamsJson,
		QueuedAt:     row.QueuedAt,
		StartedAt:    row.StartedAt,
		CompletedAt:  row.CompletedAt,
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func fromSQLiteWindowedRunsRow(row RunGetManyByJobIDWithinWindowJoinedJobsRow) RunGetManyJoinedJobsRow {
	return RunGetManyJoinedJobsRow{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   row.ParamsJson,
		QueuedAt:     row.QueuedAt,
		StartedAt:    row.StartedAt,
		CompletedAt:  row.CompletedAt,
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func fromSQLiteFilteredRunsOldestRow(row RunGetManyFilteredJoinedJobsOldestRow) RunGetManyJoinedJobsRow {
	return RunGetManyJoinedJobsRow{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   row.ParamsJson,
		QueuedAt:     row.QueuedAt,
		StartedAt:    row.StartedAt,
		CompletedAt:  row.CompletedAt,
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func fromSQLiteFilteredRunsDurationRow(row RunGetManyFilteredJoinedJobsDurationDescRow) RunGetManyJoinedJobsRow {
	return RunGetManyJoinedJobsRow{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   row.ParamsJson,
		QueuedAt:     row.QueuedAt,
		StartedAt:    row.StartedAt,
		CompletedAt:  row.CompletedAt,
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func fromSQLiteCursorNewestRow(row RunGetManyFilteredJoinedJobsCursorNewestRow) RunGetManyJoinedJobsRow {
	return RunGetManyJoinedJobsRow{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   row.ParamsJson,
		QueuedAt:     row.QueuedAt,
		StartedAt:    row.StartedAt,
		CompletedAt:  row.CompletedAt,
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func fromSQLiteCursorOldestRow(row RunGetManyFilteredJoinedJobsCursorOldestRow) RunGetManyJoinedJobsRow {
	return RunGetManyJoinedJobsRow{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		ParamsJson:   row.ParamsJson,
		QueuedAt:     row.QueuedAt,
		StartedAt:    row.StartedAt,
		CompletedAt:  row.CompletedAt,
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}
