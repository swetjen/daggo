package jobs

import (
	"context"
	"fmt"
	"time"

	"daggo/dag"
)

type UIFailureAuditOutput struct {
	Attempted bool `json:"attempted"`
}

func UIFailureAuditJob() dag.JobDefinition {
	uiFailureAudit := dag.Define[dag.NoInput, UIFailureAuditOutput]("always_fail", runAlwaysFail).
		WithDisplayName("Always Fail").
		WithDescription("Intentionally fails every execution for UI failure-state auditing.")

	return dag.NewJob("ui_failure_audit").
		WithDisplayName("UI Failure Audit").
		WithDescription("Purpose-built job that fails every run for observability and UI testing.").
		Add(uiFailureAudit).
		AddSchedule(dag.ScheduleDefinition{
			Key:         "hourly_failure",
			CronExpr:    "0 * * * *",
			Timezone:    "UTC",
			Enabled:     true,
			Description: "Run hourly and fail intentionally for UI audit scenarios.",
		}).
		MustBuild()
}

func runAlwaysFail(ctx context.Context, _ dag.NoInput) (UIFailureAuditOutput, error) {
	if err := sleepOrCancel(ctx, 150*time.Millisecond); err != nil {
		return UIFailureAuditOutput{}, err
	}
	return UIFailureAuditOutput{}, fmt.Errorf("intentional audit failure: this job is designed to fail")
}
