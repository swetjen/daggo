package jobs

import (
	"context"
	"time"

	"daggo/dag"
)

type UITransitiveAOutput struct {
	Marker string `json:"marker"`
}

type UITransitiveBInput struct {
	A UITransitiveAOutput `json:"a"`
}

type UITransitiveBOutput struct {
	Marker string `json:"marker"`
}

type UITransitiveCInput struct {
	B UITransitiveBOutput `json:"b"`
}

type UITransitiveCOutput struct {
	Marker string `json:"marker"`
}

func UITransitiveReductionJob() dag.JobDefinition {
	stepA := dag.Define[dag.NoInput, UITransitiveAOutput]("a", runUITransitiveA).
		WithDisplayName("A")
	stepB := dag.Define[UITransitiveBInput, UITransitiveBOutput]("b", runUITransitiveB).
		WithDisplayName("B")
	stepC := dag.Define[UITransitiveCInput, UITransitiveCOutput]("c", runUITransitiveC).
		WithDisplayName("C")

	return dag.NewJob("ui_transitive_reduction").
		WithDisplayName("UI Transitive Reduction").
		WithDescription("UI validation job: canonical reduced path A -> B -> C with no redundant edge.").
		Add(stepA, stepB, stepC).
		MustBuild()
}

func runUITransitiveA(ctx context.Context, _ dag.NoInput) (UITransitiveAOutput, error) {
	if err := sleepOrCancel(ctx, 1200*time.Millisecond); err != nil {
		return UITransitiveAOutput{}, err
	}
	return UITransitiveAOutput{Marker: "a"}, nil
}

func runUITransitiveB(ctx context.Context, _ UITransitiveBInput) (UITransitiveBOutput, error) {
	if err := sleepOrCancel(ctx, 1200*time.Millisecond); err != nil {
		return UITransitiveBOutput{}, err
	}
	return UITransitiveBOutput{Marker: "b"}, nil
}

func runUITransitiveC(ctx context.Context, _ UITransitiveCInput) (UITransitiveCOutput, error) {
	if err := sleepOrCancel(ctx, 1200*time.Millisecond); err != nil {
		return UITransitiveCOutput{}, err
	}
	return UITransitiveCOutput{Marker: "c"}, nil
}
