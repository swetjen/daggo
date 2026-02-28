package jobs

import (
	"context"
	"time"

	"github.com/swetjen/daggo/dag"
)

type UIWideFanoutAOutput struct {
	Marker string `json:"marker"`
}

type UIWideFanoutBInput struct {
	A UIWideFanoutAOutput `json:"a"`
}

type UIWideFanoutBOutput struct {
	Marker string `json:"marker"`
}

type UIWideFanoutCInput struct {
	A UIWideFanoutAOutput `json:"a"`
}

type UIWideFanoutCOutput struct {
	Marker string `json:"marker"`
}

type UIWideFanoutDInput struct {
	A UIWideFanoutAOutput `json:"a"`
}

type UIWideFanoutDOutput struct {
	Marker string `json:"marker"`
}

type UIWideFanoutEInput struct {
	A UIWideFanoutAOutput `json:"a"`
}

type UIWideFanoutEOutput struct {
	Marker string `json:"marker"`
}

func UIWideFanoutJob() dag.JobDefinition {
	stepA := dag.Define[dag.NoInput, UIWideFanoutAOutput]("a", runUIWideFanoutA).
		WithDisplayName("A")
	stepB := dag.Define[UIWideFanoutBInput, UIWideFanoutBOutput]("b", runUIWideFanoutB).
		WithDisplayName("B")
	stepC := dag.Define[UIWideFanoutCInput, UIWideFanoutCOutput]("c", runUIWideFanoutC).
		WithDisplayName("C")
	stepD := dag.Define[UIWideFanoutDInput, UIWideFanoutDOutput]("d", runUIWideFanoutD).
		WithDisplayName("D")
	stepE := dag.Define[UIWideFanoutEInput, UIWideFanoutEOutput]("e", runUIWideFanoutE).
		WithDisplayName("E")

	return dag.NewJob("ui_wide_fanout").
		WithDisplayName("UI Wide Fan-out").
		WithDescription("UI validation job: A fans out to B, C, D, and E.").
		Add(stepA, stepB, stepC, stepD, stepE).
		MustBuild()
}

func runUIWideFanoutA(ctx context.Context, _ dag.NoInput) (UIWideFanoutAOutput, error) {
	if err := sleepOrCancel(ctx, time.Second); err != nil {
		return UIWideFanoutAOutput{}, err
	}
	return UIWideFanoutAOutput{Marker: "a"}, nil
}

func runUIWideFanoutB(ctx context.Context, _ UIWideFanoutBInput) (UIWideFanoutBOutput, error) {
	if err := sleepOrCancel(ctx, 3*time.Second); err != nil {
		return UIWideFanoutBOutput{}, err
	}
	return UIWideFanoutBOutput{Marker: "b"}, nil
}

func runUIWideFanoutC(ctx context.Context, _ UIWideFanoutCInput) (UIWideFanoutCOutput, error) {
	if err := sleepOrCancel(ctx, 3*time.Second); err != nil {
		return UIWideFanoutCOutput{}, err
	}
	return UIWideFanoutCOutput{Marker: "c"}, nil
}

func runUIWideFanoutD(ctx context.Context, _ UIWideFanoutDInput) (UIWideFanoutDOutput, error) {
	if err := sleepOrCancel(ctx, 3*time.Second); err != nil {
		return UIWideFanoutDOutput{}, err
	}
	return UIWideFanoutDOutput{Marker: "d"}, nil
}

func runUIWideFanoutE(ctx context.Context, _ UIWideFanoutEInput) (UIWideFanoutEOutput, error) {
	if err := sleepOrCancel(ctx, 3*time.Second); err != nil {
		return UIWideFanoutEOutput{}, err
	}
	return UIWideFanoutEOutput{Marker: "e"}, nil
}
