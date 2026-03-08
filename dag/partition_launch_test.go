package dag

import (
	"slices"
	"testing"
)

func TestRenderRunTargetTagsSingleDualProjection(t *testing.T) {
	target := RunTarget{
		Selection: NormalizedPartitionSelection{
			Mode:   PartitionSelectionModeSingle,
			Keys:   []string{"2026-01-01"},
			Ranges: []PartitionSelectionRange{{StartKey: "2026-01-01", EndKey: "2026-01-01"}},
		},
		BackfillKey: "bf-123",
	}

	tags, err := RenderRunTargetTags(target, PartitionTagProjectionDual)
	if err != nil {
		t.Fatalf("render tags: %v", err)
	}
	if tags[TagDaggoPartition] != "2026-01-01" {
		t.Fatalf("missing daggo partition tag: %+v", tags)
	}
	if tags[TagDagsterPartition] != "2026-01-01" {
		t.Fatalf("missing dagster partition tag: %+v", tags)
	}
	if tags[TagDaggoBackfill] != "bf-123" || tags[TagDagsterBackfill] != "bf-123" {
		t.Fatalf("missing backfill tags: %+v", tags)
	}
}

func TestRenderRunTargetTagsSubsetDagsterProjectionFails(t *testing.T) {
	target := RunTarget{
		Selection: NormalizedPartitionSelection{
			Mode:   PartitionSelectionModeSubset,
			Keys:   []string{"a", "c"},
			Ranges: []PartitionSelectionRange{{StartKey: "a", EndKey: "a"}, {StartKey: "c", EndKey: "c"}},
		},
	}
	_, err := RenderRunTargetTags(target, PartitionTagProjectionDagster)
	if err == nil {
		t.Fatalf("expected error for dagster projection of non-contiguous subset")
	}
}

func TestParseRunTargetTagsSingle(t *testing.T) {
	parsed, err := ParseRunTargetTags(map[string]string{
		TagDaggoPartition: "k1",
		TagDaggoBackfill:  "bf-1",
	})
	if err != nil {
		t.Fatalf("parse tags: %v", err)
	}
	if parsed.Selection.Mode != PartitionSelectionModeSingle {
		t.Fatalf("expected single mode, got %q", parsed.Selection.Mode)
	}
	if parsed.BackfillKey != "bf-1" {
		t.Fatalf("expected backfill key bf-1, got %q", parsed.BackfillKey)
	}
}

func TestBuildMaterializationPlanUsesSingleRunPerPartition(t *testing.T) {
	selection, err := NormalizePartitionSelection(
		[]string{"a", "b", "c"},
		SelectPartitionRange("a", "c"),
	)
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}
	plan, err := BuildMaterializationPlan(selection)
	if err != nil {
		t.Fatalf("build materialization plan: %v", err)
	}
	if len(plan.RunTargets) != 3 {
		t.Fatalf("expected 3 run targets, got %d", len(plan.RunTargets))
	}
	for _, target := range plan.RunTargets {
		if target.Selection.Mode != PartitionSelectionModeSingle {
			t.Fatalf("expected single mode target, got %q", target.Selection.Mode)
		}
	}
}

func TestBuildBackfillPlanSingleRun(t *testing.T) {
	selection, err := NormalizePartitionSelection(
		[]string{"a", "b", "c"},
		SelectPartitionRange("a", "c"),
	)
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}
	plan, err := BuildBackfillPlan(selection, BackfillPolicy{Mode: BackfillPolicySingleRun})
	if err != nil {
		t.Fatalf("build backfill plan: %v", err)
	}
	if len(plan.RunTargets) != 1 {
		t.Fatalf("expected 1 run target, got %d", len(plan.RunTargets))
	}
	if plan.RunTargets[0].Selection.Mode != PartitionSelectionModeRange {
		t.Fatalf("expected range mode, got %q", plan.RunTargets[0].Selection.Mode)
	}
}

func TestBuildBackfillPlanMultiRunChunksContiguousRanges(t *testing.T) {
	selection, err := NormalizePartitionSelection(
		[]string{"a", "b", "c", "d", "e"},
		SelectPartitionRange("a", "e"),
	)
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}
	plan, err := BuildBackfillPlan(selection, BackfillPolicy{
		Mode:                BackfillPolicyMultiRun,
		MaxPartitionsPerRun: 2,
	})
	if err != nil {
		t.Fatalf("build backfill plan: %v", err)
	}
	if len(plan.RunTargets) != 3 {
		t.Fatalf("expected 3 run targets, got %d", len(plan.RunTargets))
	}
	expected := [][]string{{"a", "b"}, {"c", "d"}, {"e"}}
	for index, target := range plan.RunTargets {
		if !slices.Equal(target.Selection.Keys, expected[index]) {
			t.Fatalf("run target %d keys mismatch: expected %v, got %v", index, expected[index], target.Selection.Keys)
		}
	}
}

func TestBuildBackfillPlanMultiRunForNonContiguousSelection(t *testing.T) {
	selection := MergePartitionSelections(
		SelectPartitionKey("a"),
		SelectPartitionRange("c", "d"),
	)
	normalized, err := NormalizePartitionSelection(
		[]string{"a", "b", "c", "d"},
		selection,
	)
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}
	plan, err := BuildBackfillPlan(normalized, BackfillPolicy{
		Mode:                BackfillPolicyMultiRun,
		MaxPartitionsPerRun: 2,
	})
	if err != nil {
		t.Fatalf("build backfill plan: %v", err)
	}
	if len(plan.RunTargets) != 2 {
		t.Fatalf("expected 2 run targets, got %d", len(plan.RunTargets))
	}
	first := plan.RunTargets[0].Selection.Keys
	second := plan.RunTargets[1].Selection.Keys
	if !slices.Equal(first, []string{"a"}) || !slices.Equal(second, []string{"c", "d"}) {
		t.Fatalf("unexpected run targets: first=%v second=%v", first, second)
	}
}
