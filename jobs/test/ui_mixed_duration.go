package jobs

import (
	"context"
	"time"

	"daggo/dag"
)

type UIMixedAOutput struct {
	Marker string `json:"marker"`
}

type UIMixedBInput struct {
	A UIMixedAOutput `json:"a"`
}

type UIMixedBOutput struct {
	Marker string `json:"marker"`
}

type UIMixedXOutput struct {
	Marker string `json:"marker"`
}

type UIMixedYInput struct {
	X UIMixedXOutput `json:"x"`
}

type UIMixedYOutput struct {
	Marker string `json:"marker"`
}

func UIMixedDurationJob() dag.JobDefinition {
	stepA := dag.Define[dag.NoInput, UIMixedAOutput]("a", runUIMixedA).
		WithDisplayName("A (short)")
	stepB := dag.Define[UIMixedBInput, UIMixedBOutput]("b", runUIMixedB).
		WithDisplayName("B (short)")
	stepX := dag.Define[dag.NoInput, UIMixedXOutput]("x", runUIMixedX).
		WithDisplayName("X (long)")
	stepY := dag.Define[UIMixedYInput, UIMixedYOutput]("y", runUIMixedY).
		WithDisplayName("Y (long)")

	return dag.NewJob("ui_mixed_duration").
		WithDisplayName("UI Mixed Duration").
		WithDescription("UI validation job: short A->B path and long X->Y path for marker width scaling.").
		Add(stepA, stepB, stepX, stepY).
		MustBuild()
}

func runUIMixedA(ctx context.Context, _ dag.NoInput) (UIMixedAOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UIMixedAOutput{}, err
	}
	return UIMixedAOutput{Marker: "a"}, nil
}

func runUIMixedB(ctx context.Context, _ UIMixedBInput) (UIMixedBOutput, error) {
	if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
		return UIMixedBOutput{}, err
	}
	return UIMixedBOutput{Marker: "b"}, nil
}

func runUIMixedX(ctx context.Context, _ dag.NoInput) (UIMixedXOutput, error) {
	if err := sleepOrCancel(ctx, 15*time.Minute); err != nil {
		return UIMixedXOutput{}, err
	}
	return UIMixedXOutput{Marker: "x"}, nil
}

func runUIMixedY(ctx context.Context, _ UIMixedYInput) (UIMixedYOutput, error) {
	if err := sleepOrCancel(ctx, 10*time.Minute); err != nil {
		return UIMixedYOutput{}, err
	}
	return UIMixedYOutput{Marker: "y"}, nil
}
