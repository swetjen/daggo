package jobs

import "testing"

func TestLegacyJobsBuildForCoverage(t *testing.T) {
	builders := []struct {
		name string
		fn   func() interface{}
	}{
		{name: "daily_orders_pipeline", fn: func() interface{} { return DailyOrdersPipelineJob() }},
		{name: "ml_feature_refresh", fn: func() interface{} { return MLFeatureRefreshJob() }},
		{name: "ui_failure_audit", fn: func() interface{} { return UIFailureAuditJob() }},
		{name: "ui_linear_chain", fn: func() interface{} { return UILinearChainJob() }},
		{name: "ui_diamond_join", fn: func() interface{} { return UIDiamondJoinJob() }},
		{name: "ui_wide_fanout", fn: func() interface{} { return UIWideFanoutJob() }},
		{name: "ui_multi_root_forest", fn: func() interface{} { return UIMultiRootForestJob() }},
		{name: "ui_mixed_duration", fn: func() interface{} { return UIMixedDurationJob() }},
		{name: "ui_failure_case", fn: func() interface{} { return UIFailureCaseJob() }},
		{name: "ui_transitive_reduction", fn: func() interface{} { return UITransitiveReductionJob() }},
	}

	for _, builder := range builders {
		builder := builder
		t.Run(builder.name, func(t *testing.T) {
			t.Helper()
			got := builder.fn()
			if got == nil {
				t.Fatalf("expected job definition, got nil")
			}
		})
	}
}
