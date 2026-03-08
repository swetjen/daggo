package dag

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestResolveUpstreamMappedPartitionsIdentityIncludesMissing(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)

	downstreamDefinition, err := NewStaticPartitionDefinition([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("create downstream definition: %v", err)
	}
	upstreamDefinition, err := NewStaticPartitionDefinition([]string{"a", "b"})
	if err != nil {
		t.Fatalf("create upstream definition: %v", err)
	}
	selection, err := NormalizePartitionSelectionForDefinition(
		ctx,
		downstreamDefinition,
		SelectPartitionSubset("a", "c"),
		now,
	)
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}

	result, err := ResolveUpstreamMappedPartitionsForSelection(
		ctx,
		IdentityPartitionMapping{},
		selection,
		downstreamDefinition,
		upstreamDefinition,
		now,
	)
	if err != nil {
		t.Fatalf("resolve mapping: %v", err)
	}
	if !slices.Equal(result.PartitionKeys, []string{"a"}) {
		t.Fatalf("expected existent [a], got %v", result.PartitionKeys)
	}
	if !slices.Equal(result.RequiredButNonexistentKeys, []string{"c"}) {
		t.Fatalf("expected missing [c], got %v", result.RequiredButNonexistentKeys)
	}
}

func TestResolveUpstreamMappedPartitionsStaticMapping(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)

	downstreamDefinition, err := NewStaticPartitionDefinition([]string{"north", "south"})
	if err != nil {
		t.Fatalf("create downstream definition: %v", err)
	}
	upstreamDefinition, err := NewStaticPartitionDefinition([]string{"us-east", "us-west"})
	if err != nil {
		t.Fatalf("create upstream definition: %v", err)
	}
	selection, err := NormalizePartitionSelectionForDefinition(
		ctx,
		downstreamDefinition,
		SelectPartitionSubset("north", "south"),
		now,
	)
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}

	result, err := ResolveUpstreamMappedPartitionsForSelection(
		ctx,
		StaticPartitionMapping{
			DownstreamToUpstream: map[string][]string{
				"north": {"us-east"},
				"south": {"us-central"},
			},
		},
		selection,
		downstreamDefinition,
		upstreamDefinition,
		now,
	)
	if err != nil {
		t.Fatalf("resolve mapping: %v", err)
	}
	if !slices.Equal(result.PartitionKeys, []string{"us-east"}) {
		t.Fatalf("expected existent [us-east], got %v", result.PartitionKeys)
	}
	if !slices.Equal(result.RequiredButNonexistentKeys, []string{"us-central"}) {
		t.Fatalf("expected missing [us-central], got %v", result.RequiredButNonexistentKeys)
	}
}

func TestResolveUpstreamMappedPartitionsTimeWindowSurfacesMissing(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.January, 2, 23, 0, 0, 0, time.UTC)

	downstreamDefinition := TimeWindowPartitionDefinition{
		Cadence:  TimePartitionCadenceDaily,
		Start:    time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		Location: time.UTC,
	}
	upstreamDefinition := TimeWindowPartitionDefinition{
		Cadence:  TimePartitionCadenceHourly,
		Start:    time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC),
		Location: time.UTC,
	}
	selection, err := NormalizePartitionSelectionForDefinition(
		ctx,
		downstreamDefinition,
		SelectPartitionKey("2026-01-01"),
		now,
	)
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}
	result, err := ResolveUpstreamMappedPartitionsForSelection(
		ctx,
		TimeWindowPartitionMapping{},
		selection,
		downstreamDefinition,
		upstreamDefinition,
		now,
	)
	if err != nil {
		t.Fatalf("resolve mapping: %v", err)
	}
	if len(result.PartitionKeys) != 0 {
		t.Fatalf("expected no existent mapped keys, got %v", result.PartitionKeys)
	}
	if len(result.RequiredButNonexistentKeys) != 24 {
		t.Fatalf("expected 24 missing hourly keys, got %d", len(result.RequiredButNonexistentKeys))
	}
	if result.RequiredButNonexistentKeys[0] != "2026-01-01T00" || result.RequiredButNonexistentKeys[len(result.RequiredButNonexistentKeys)-1] != "2026-01-01T23" {
		t.Fatalf("unexpected missing hourly range: first=%s last=%s", result.RequiredButNonexistentKeys[0], result.RequiredButNonexistentKeys[len(result.RequiredButNonexistentKeys)-1])
	}
}

func TestResolveUpstreamMappedPartitionsMultiMapping(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)

	dateDownstream, err := NewStaticPartitionDefinition([]string{"2026-01-01"})
	if err != nil {
		t.Fatalf("create downstream date definition: %v", err)
	}
	regionDownstream, err := NewStaticPartitionDefinition([]string{"us", "eu"})
	if err != nil {
		t.Fatalf("create downstream region definition: %v", err)
	}
	downstreamDefinition, err := NewMultiPartitionDefinition(map[string]PartitionDefinition{
		"date":   dateDownstream,
		"region": regionDownstream,
	})
	if err != nil {
		t.Fatalf("create downstream multi definition: %v", err)
	}

	dateUpstream, err := NewStaticPartitionDefinition([]string{"2026-01-01"})
	if err != nil {
		t.Fatalf("create upstream date definition: %v", err)
	}
	regionUpstream, err := NewStaticPartitionDefinition([]string{"us"})
	if err != nil {
		t.Fatalf("create upstream region definition: %v", err)
	}
	upstreamDefinition, err := NewMultiPartitionDefinition(map[string]PartitionDefinition{
		"date":   dateUpstream,
		"region": regionUpstream,
	})
	if err != nil {
		t.Fatalf("create upstream multi definition: %v", err)
	}

	downstreamKeys, err := downstreamDefinition.PartitionKeys(ctx, now)
	if err != nil {
		t.Fatalf("get downstream keys: %v", err)
	}
	selection, err := NormalizePartitionSelection(downstreamKeys, SelectPartitionKey("2026-01-01|eu"))
	if err != nil {
		t.Fatalf("normalize selection: %v", err)
	}

	identityResult, err := ResolveUpstreamMappedPartitionsForSelection(
		ctx,
		MultiPartitionMapping{},
		selection,
		downstreamDefinition,
		upstreamDefinition,
		now,
	)
	if err != nil {
		t.Fatalf("resolve identity multi mapping: %v", err)
	}
	if len(identityResult.PartitionKeys) != 0 {
		t.Fatalf("expected no existent key for identity mapping, got %v", identityResult.PartitionKeys)
	}
	if !slices.Equal(identityResult.RequiredButNonexistentKeys, []string{"2026-01-01|eu"}) {
		t.Fatalf("expected missing [2026-01-01|eu], got %v", identityResult.RequiredButNonexistentKeys)
	}

	explicitResult, err := ResolveUpstreamMappedPartitionsForSelection(
		ctx,
		MultiPartitionMapping{
			DimensionMappings: []MultiPartitionDimensionMapping{
				{
					UpstreamDimension:   "region",
					DownstreamDimension: "region",
					Mapping: StaticPartitionMapping{
						DownstreamToUpstream: map[string][]string{
							"eu": {"us"},
						},
					},
				},
			},
		},
		selection,
		downstreamDefinition,
		upstreamDefinition,
		now,
	)
	if err != nil {
		t.Fatalf("resolve explicit multi mapping: %v", err)
	}
	if !slices.Equal(explicitResult.PartitionKeys, []string{"2026-01-01|us"}) {
		t.Fatalf("expected existent [2026-01-01|us], got %v", explicitResult.PartitionKeys)
	}
	if len(explicitResult.RequiredButNonexistentKeys) != 0 {
		t.Fatalf("expected no missing keys, got %v", explicitResult.RequiredButNonexistentKeys)
	}
}
