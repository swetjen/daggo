package jobs

import (
	"context"
	"time"

	"github.com/swetjen/daggo/dag"
)

type ExtractFeaturesOutput struct {
	FeatureRows int    `json:"feature_rows"`
	Window      string `json:"window"`
}

type LongRefreshSleepInput struct {
	Features ExtractFeaturesOutput `json:"features"`
}

type LongRefreshSleepOutput struct {
	SleptMinutes int `json:"slept_minutes"`
}

type TrainModelInput struct {
	Features ExtractFeaturesOutput  `json:"features"`
	Sleep    LongRefreshSleepOutput `json:"sleep"`
}

type TrainModelOutput struct {
	ModelID      string `json:"model_id"`
	TrainSeconds int    `json:"train_seconds"`
}

type EvaluateModelInput struct {
	Model TrainModelOutput `json:"model"`
}

type EvaluateModelOutput struct {
	AUC          float64 `json:"auc"`
	LatencyMsP95 float64 `json:"latency_ms_p95"`
	Accepted     bool    `json:"accepted"`
}

type DeployModelInput struct {
	Evaluation EvaluateModelOutput `json:"evaluation"`
}

type DeployModelOutput struct {
	Deployment     string `json:"deployment"`
	TrafficPercent int    `json:"traffic_percent"`
}

func MLFeatureRefreshJob() dag.JobDefinition {
	extractFeatures := dag.Define[dag.NoInput, ExtractFeaturesOutput]("extract_features", runExtractFeatures).
		WithDisplayName("Extract Features").
		WithDescription("Fetch and materialize model feature vectors.")
	longRefreshSleep := dag.Define[LongRefreshSleepInput, LongRefreshSleepOutput]("long_refresh_sleep", runLongRefreshSleep).
		WithDisplayName("Long Refresh Sleep").
		WithDescription("Intentionally sleep 5-15 minutes to emulate longer-running orchestration work.")
	trainModel := dag.Define[TrainModelInput, TrainModelOutput]("train_model", runTrainModel).
		WithDisplayName("Train Model").
		WithDescription("Train a fresh model candidate from feature vectors.")
	evaluateModel := dag.Define[EvaluateModelInput, EvaluateModelOutput]("evaluate_model", runEvaluateModel).
		WithDisplayName("Evaluate Model").
		WithDescription("Score model quality against acceptance thresholds.")
	deployModel := dag.Define[DeployModelInput, DeployModelOutput]("deploy_model", runDeployModel).
		WithDisplayName("Deploy Model").
		WithDescription("Promote the accepted model candidate to production.")

	return dag.NewJob("ml_feature_refresh").
		WithDisplayName("ML Feature Refresh").
		WithDescription("Build and evaluate a candidate model from fresh features.").
		Add(extractFeatures, longRefreshSleep, trainModel, evaluateModel, deployModel).
		AddSchedule(dag.ScheduleDefinition{
			Key:         "nightly_refresh",
			CronExpr:    "0 * * * *",
			Timezone:    "UTC",
			Enabled:     true,
			Description: "Run at the top of every hour.",
		}).
		MustBuild()
}

func runExtractFeatures(ctx context.Context, _ dag.NoInput) (ExtractFeaturesOutput, error) {
	if err := sleepOrCancel(ctx, 450*time.Millisecond); err != nil {
		return ExtractFeaturesOutput{}, err
	}
	return ExtractFeaturesOutput{FeatureRows: 9800, Window: "24h"}, nil
}

func runLongRefreshSleep(ctx context.Context, _ LongRefreshSleepInput) (LongRefreshSleepOutput, error) {
	delay := randomMinuteDuration(5, 15)
	if err := sleepOrCancel(ctx, delay); err != nil {
		return LongRefreshSleepOutput{}, err
	}
	return LongRefreshSleepOutput{SleptMinutes: int(delay / time.Minute)}, nil
}

func runTrainModel(ctx context.Context, _ TrainModelInput) (TrainModelOutput, error) {
	if err := sleepOrCancel(ctx, 560*time.Millisecond); err != nil {
		return TrainModelOutput{}, err
	}
	return TrainModelOutput{ModelID: "model_v20260221", TrainSeconds: 38}, nil
}

func runEvaluateModel(ctx context.Context, _ EvaluateModelInput) (EvaluateModelOutput, error) {
	if err := sleepOrCancel(ctx, 370*time.Millisecond); err != nil {
		return EvaluateModelOutput{}, err
	}
	return EvaluateModelOutput{AUC: 0.91, LatencyMsP95: 18.2, Accepted: true}, nil
}

func runDeployModel(ctx context.Context, _ DeployModelInput) (DeployModelOutput, error) {
	if err := sleepOrCancel(ctx, 300*time.Millisecond); err != nil {
		return DeployModelOutput{}, err
	}
	return DeployModelOutput{Deployment: "completed", TrafficPercent: 100}, nil
}
