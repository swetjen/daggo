package jobs

import (
	"context"
	"time"

	"daggo/dag"
)

type UILinearAOutput struct {
	Marker string `json:"marker"`
}

type UILinearBInput struct {
	A UILinearAOutput `json:"a"`
}

type UILinearBOutput struct {
	Marker string `json:"marker"`
}

type UILinearCInput struct {
	B UILinearBOutput `json:"b"`
}

type UILinearCOutput struct {
	Marker string `json:"marker"`
}

type UILinearDInput struct {
	C UILinearCOutput `json:"c"`
}

type UILinearDOutput struct {
	Marker string `json:"marker"`
}

func UILinearChainJob() dag.JobDefinition {
	stepA := dag.Define[dag.NoInput, UILinearAOutput]("a", runUILinearA).
		WithDisplayName("A")
	stepB := dag.Define[UILinearBInput, UILinearBOutput]("b", runUILinearB).
		WithDisplayName("B")
	stepC := dag.Define[UILinearCInput, UILinearCOutput]("c", runUILinearC).
		WithDisplayName("C")
	stepD := dag.Define[UILinearDInput, UILinearDOutput]("d", runUILinearD).
		WithDisplayName("D")

	return dag.NewJob("ui_linear_chain").
		WithDisplayName("UI Linear Chain").
		WithDescription("UI validation job: A -> B -> C -> D with deterministic two-second steps.").
		Add(stepA, stepB, stepC, stepD).
		MustBuild()
}

func runUILinearA(ctx context.Context, _ dag.NoInput) (UILinearAOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UILinearAOutput{}, err
	}
	return UILinearAOutput{Marker: "a"}, nil
}

func runUILinearB(ctx context.Context, _ UILinearBInput) (UILinearBOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UILinearBOutput{}, err
	}
	return UILinearBOutput{Marker: "b"}, nil
}

func runUILinearC(ctx context.Context, _ UILinearCInput) (UILinearCOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UILinearCOutput{}, err
	}
	return UILinearCOutput{Marker: "c"}, nil
}

func runUILinearD(ctx context.Context, _ UILinearDInput) (UILinearDOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UILinearDOutput{}, err
	}
	return UILinearDOutput{Marker: "d"}, nil
}
