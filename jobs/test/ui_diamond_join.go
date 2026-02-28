package jobs

import (
	"context"
	"time"

	"github.com/swetjen/daggo/dag"
)

type UIDiamondAOutput struct {
	Marker string `json:"marker"`
}

type UIDiamondBInput struct {
	A UIDiamondAOutput `json:"a"`
}

type UIDiamondBOutput struct {
	Marker string `json:"marker"`
}

type UIDiamondCInput struct {
	A UIDiamondAOutput `json:"a"`
}

type UIDiamondCOutput struct {
	Marker string `json:"marker"`
}

type UIDiamondDInput struct {
	B UIDiamondBOutput `json:"b"`
	C UIDiamondCOutput `json:"c"`
}

type UIDiamondDOutput struct {
	Marker string `json:"marker"`
}

func UIDiamondJoinJob() dag.JobDefinition {
	stepA := dag.Define[dag.NoInput, UIDiamondAOutput]("a", runUIDiamondA).
		WithDisplayName("A")
	stepB := dag.Define[UIDiamondBInput, UIDiamondBOutput]("b", runUIDiamondB).
		WithDisplayName("B")
	stepC := dag.Define[UIDiamondCInput, UIDiamondCOutput]("c", runUIDiamondC).
		WithDisplayName("C")
	stepD := dag.Define[UIDiamondDInput, UIDiamondDOutput]("d", runUIDiamondD).
		WithDisplayName("D")

	return dag.NewJob("ui_diamond_join").
		WithDisplayName("UI Diamond Join").
		WithDescription("UI validation job: A fans out to B and C, then joins at D.").
		Add(stepA, stepB, stepC, stepD).
		MustBuild()
}

func runUIDiamondA(ctx context.Context, _ dag.NoInput) (UIDiamondAOutput, error) {
	if err := sleepOrCancel(ctx, time.Second); err != nil {
		return UIDiamondAOutput{}, err
	}
	return UIDiamondAOutput{Marker: "a"}, nil
}

func runUIDiamondB(ctx context.Context, _ UIDiamondBInput) (UIDiamondBOutput, error) {
	if err := sleepOrCancel(ctx, 5*time.Second); err != nil {
		return UIDiamondBOutput{}, err
	}
	return UIDiamondBOutput{Marker: "b"}, nil
}

func runUIDiamondC(ctx context.Context, _ UIDiamondCInput) (UIDiamondCOutput, error) {
	if err := sleepOrCancel(ctx, 5*time.Second); err != nil {
		return UIDiamondCOutput{}, err
	}
	return UIDiamondCOutput{Marker: "c"}, nil
}

func runUIDiamondD(ctx context.Context, _ UIDiamondDInput) (UIDiamondDOutput, error) {
	if err := sleepOrCancel(ctx, time.Second); err != nil {
		return UIDiamondDOutput{}, err
	}
	return UIDiamondDOutput{Marker: "d"}, nil
}
