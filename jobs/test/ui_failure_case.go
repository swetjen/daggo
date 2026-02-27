package jobs

import (
	"context"
	"fmt"
	"time"

	"daggo/dag"
)

type UIFailureAOutput struct {
	Marker string `json:"marker"`
}

type UIFailureBInput struct {
	A UIFailureAOutput `json:"a"`
}

type UIFailureBOutput struct {
	Marker string `json:"marker"`
}

type UIFailureCInput struct {
	B UIFailureBOutput `json:"b"`
}

type UIFailureCOutput struct {
	Marker string `json:"marker"`
}

func UIFailureCaseJob() dag.JobDefinition {
	stepA := dag.Define[dag.NoInput, UIFailureAOutput]("a", runUIFailureA).
		WithDisplayName("A")
	stepB := dag.Define[UIFailureBInput, UIFailureBOutput]("b", runUIFailureB).
		WithDisplayName("B")
	stepC := dag.Define[UIFailureCInput, UIFailureCOutput]("c", runUIFailureC).
		WithDisplayName("C (fails)")

	return dag.NewJob("ui_failure_case").
		WithDisplayName("UI Failure Case").
		WithDescription("UI validation job: A -> B -> C where C fails deterministically.").
		Add(stepA, stepB, stepC).
		MustBuild()
}

func runUIFailureA(ctx context.Context, _ dag.NoInput) (UIFailureAOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UIFailureAOutput{}, err
	}
	return UIFailureAOutput{Marker: "a"}, nil
}

func runUIFailureB(ctx context.Context, _ UIFailureBInput) (UIFailureBOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UIFailureBOutput{}, err
	}
	return UIFailureBOutput{Marker: "b"}, nil
}

func runUIFailureC(ctx context.Context, _ UIFailureCInput) (UIFailureCOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UIFailureCOutput{}, err
	}
	return UIFailureCOutput{}, fmt.Errorf("intentional ui failure case: step c always fails")
}
