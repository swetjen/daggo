package jobs

import (
	"context"
	"time"

	"github.com/swetjen/daggo/dag"
)

type IngestOrdersOutput struct {
	RecordsIngested int    `json:"records_ingested"`
	Source          string `json:"source"`
}

type CleanOrdersInput struct {
	Ingest IngestOrdersOutput `json:"ingest"`
}

type CleanOrdersOutput struct {
	RecordsCleaned int `json:"records_cleaned"`
	DroppedRecords int `json:"dropped_records"`
}

type AggregateOrdersInput struct {
	Clean CleanOrdersOutput `json:"clean"`
}

type AggregateOrdersOutput struct {
	RecordsAggregated int     `json:"records_aggregated"`
	DailyRevenue      float64 `json:"daily_revenue"`
}

type PublishDashboardInput struct {
	Aggregate AggregateOrdersOutput `json:"aggregate"`
}

type PublishDashboardOutput struct {
	DashboardVersion string `json:"dashboard_version"`
	CacheRefresh     string `json:"cache_refresh"`
}

func DailyOrdersPipelineJob() dag.JobDefinition {
	ingestOrders := dag.Op[dag.NoInput, IngestOrdersOutput]("ingest_orders", runIngestOrders).
		WithDisplayName("Ingest Orders").
		WithDescription("Pull fresh raw orders from source systems.")
	cleanOrders := dag.Op[CleanOrdersInput, CleanOrdersOutput]("clean_orders", runCleanOrders).
		WithDisplayName("Clean Orders").
		WithDescription("Normalize and validate the ingested payloads.")
	aggregateOrders := dag.Op[AggregateOrdersInput, AggregateOrdersOutput]("aggregate_orders", runAggregateOrders).
		WithDisplayName("Aggregate Orders").
		WithDescription("Compute daily aggregates for analytics.")
	publishDashboard := dag.Op[PublishDashboardInput, PublishDashboardOutput]("publish_dashboard", runPublishDashboard).
		WithDisplayName("Publish Dashboard").
		WithDescription("Publish fresh aggregate metrics to the dashboard cache.")

	return dag.NewJob("daily_orders_pipeline").
		WithDisplayName("Daily Orders Pipeline").
		WithDescription("Ingest, clean, aggregate, and publish order metrics.").
		Add(ingestOrders, cleanOrders, aggregateOrders, publishDashboard).
		AddSchedule(dag.ScheduleDefinition{
			Key:         "every_five_minutes",
			CronExpr:    "*/5 * * * *",
			Timezone:    "UTC",
			Enabled:     true,
			Description: "Refresh every 5 minutes.",
		}).
		MustBuild()
}

func runIngestOrders(ctx context.Context, _ dag.NoInput) (IngestOrdersOutput, error) {
	if err := sleepOrCancel(ctx, 420*time.Millisecond); err != nil {
		return IngestOrdersOutput{}, err
	}
	return IngestOrdersOutput{RecordsIngested: 1240, Source: "orders_api"}, nil
}

func runCleanOrders(ctx context.Context, input CleanOrdersInput) (CleanOrdersOutput, error) {
	if err := sleepOrCancel(ctx, 340*time.Millisecond); err != nil {
		return CleanOrdersOutput{}, err
	}
	cleaned := input.Ingest.RecordsIngested - 36
	if cleaned < 0 {
		cleaned = 0
	}
	return CleanOrdersOutput{
		RecordsCleaned: cleaned,
		DroppedRecords: input.Ingest.RecordsIngested - cleaned,
	}, nil
}

func runAggregateOrders(ctx context.Context, input AggregateOrdersInput) (AggregateOrdersOutput, error) {
	if err := sleepOrCancel(ctx, 360*time.Millisecond); err != nil {
		return AggregateOrdersOutput{}, err
	}
	return AggregateOrdersOutput{
		RecordsAggregated: input.Clean.RecordsCleaned,
		DailyRevenue:      float64(input.Clean.RecordsCleaned) * 51.35,
	}, nil
}

func runPublishDashboard(ctx context.Context, _ PublishDashboardInput) (PublishDashboardOutput, error) {
	if err := sleepOrCancel(ctx, 290*time.Millisecond); err != nil {
		return PublishDashboardOutput{}, err
	}
	return PublishDashboardOutput{
		DashboardVersion: time.Now().UTC().Format(time.RFC3339),
		CacheRefresh:     "ok",
	}, nil
}
