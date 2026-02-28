package jobs

import (
	"context"
	"time"

	"github.com/swetjen/daggo/dag"
)

type UIMultiRootAOutput struct {
	Marker string `json:"marker"`
}

type UIMultiRootBInput struct {
	A UIMultiRootAOutput `json:"a"`
}

type UIMultiRootBOutput struct {
	Marker string `json:"marker"`
}

type UIMultiRootCOutput struct {
	Marker string `json:"marker"`
}

type UIMultiRootDInput struct {
	C UIMultiRootCOutput `json:"c"`
}

type UIMultiRootDOutput struct {
	Marker string `json:"marker"`
}

func UIMultiRootForestJob() dag.JobDefinition {
	stepA := dag.Define[dag.NoInput, UIMultiRootAOutput]("a", runUIMultiRootA).
		WithDisplayName("A")
	stepB := dag.Define[UIMultiRootBInput, UIMultiRootBOutput]("b", runUIMultiRootB).
		WithDisplayName("B")
	stepC := dag.Define[dag.NoInput, UIMultiRootCOutput]("c", runUIMultiRootC).
		WithDisplayName("C")
	stepD := dag.Define[UIMultiRootDInput, UIMultiRootDOutput]("d", runUIMultiRootD).
		WithDisplayName("D")

	return dag.NewJob("ui_multi_root_forest").
		WithDisplayName("UI Multi-root Forest").
		WithDescription("UI validation job: two independent root chains A->B and C->D.").
		Add(stepA, stepB, stepC, stepD).
		MustBuild()
}

func runUIMultiRootA(ctx context.Context, _ dag.NoInput) (UIMultiRootAOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UIMultiRootAOutput{}, err
	}
	return UIMultiRootAOutput{Marker: "a"}, nil
}

func runUIMultiRootB(ctx context.Context, _ UIMultiRootBInput) (UIMultiRootBOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UIMultiRootBOutput{}, err
	}
	return UIMultiRootBOutput{Marker: "b"}, nil
}

func runUIMultiRootC(ctx context.Context, _ dag.NoInput) (UIMultiRootCOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UIMultiRootCOutput{}, err
	}
	return UIMultiRootCOutput{Marker: "c"}, nil
}

func runUIMultiRootD(ctx context.Context, _ UIMultiRootDInput) (UIMultiRootDOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UIMultiRootDOutput{}, err
	}
	return UIMultiRootDOutput{Marker: "d"}, nil
}
