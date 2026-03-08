package dag

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	TagDaggoPartition             = "daggo/partition"
	TagDaggoPartitionRangeStart   = "daggo/asset_partition_range_start"
	TagDaggoPartitionRangeEnd     = "daggo/asset_partition_range_end"
	TagDaggoPartitionSubsetJSON   = "daggo/partition_subset_json"
	TagDaggoBackfill              = "daggo/backfill"
	TagDagsterPartition           = "dagster/partition"
	TagDagsterPartitionRangeStart = "dagster/asset_partition_range_start"
	TagDagsterPartitionRangeEnd   = "dagster/asset_partition_range_end"
	TagDagsterBackfill            = "dagster/backfill"
)

type PartitionTagProjectionStrategy string

const (
	PartitionTagProjectionDaggo   PartitionTagProjectionStrategy = "daggo"
	PartitionTagProjectionDual    PartitionTagProjectionStrategy = "dual"
	PartitionTagProjectionDagster PartitionTagProjectionStrategy = "dagster"
)

type RunTarget struct {
	Selection   NormalizedPartitionSelection
	BackfillKey string
}

func RenderRunTargetTags(target RunTarget, strategy PartitionTagProjectionStrategy) (map[string]string, error) {
	if len(target.Selection.Keys) == 0 {
		return nil, fmt.Errorf("run target selection is empty")
	}

	switch strategy {
	case PartitionTagProjectionDaggo, PartitionTagProjectionDual, PartitionTagProjectionDagster:
	default:
		return nil, fmt.Errorf("unsupported tag projection strategy %q", strategy)
	}

	tags := make(map[string]string)
	if key := strings.TrimSpace(target.BackfillKey); key != "" {
		tags[TagDaggoBackfill] = key
		if strategy == PartitionTagProjectionDual || strategy == PartitionTagProjectionDagster {
			tags[TagDagsterBackfill] = key
		}
	}

	switch target.Selection.Mode {
	case PartitionSelectionModeSingle:
		key := target.Selection.Keys[0]
		tags[TagDaggoPartition] = key
		if strategy == PartitionTagProjectionDual || strategy == PartitionTagProjectionDagster {
			tags[TagDagsterPartition] = key
		}
	case PartitionSelectionModeRange:
		if len(target.Selection.Ranges) != 1 {
			return nil, fmt.Errorf("range selection must include exactly one contiguous range")
		}
		r := target.Selection.Ranges[0]
		tags[TagDaggoPartitionRangeStart] = r.StartKey
		tags[TagDaggoPartitionRangeEnd] = r.EndKey
		if strategy == PartitionTagProjectionDual || strategy == PartitionTagProjectionDagster {
			tags[TagDagsterPartitionRangeStart] = r.StartKey
			tags[TagDagsterPartitionRangeEnd] = r.EndKey
		}
	case PartitionSelectionModeSubset:
		payload, err := json.Marshal(target.Selection.Keys)
		if err != nil {
			return nil, err
		}
		tags[TagDaggoPartitionSubsetJSON] = string(payload)
		if strategy == PartitionTagProjectionDagster || strategy == PartitionTagProjectionDual {
			return nil, fmt.Errorf("dagster projection requires contiguous range or single partition; split subset into multiple run targets")
		}
	default:
		return nil, fmt.Errorf("unsupported selection mode %q", target.Selection.Mode)
	}
	return tags, nil
}

type ParsedRunTargetTags struct {
	Selection   NormalizedPartitionSelection
	BackfillKey string
	Namespace   string
}

func ParseRunTargetTags(tags map[string]string) (ParsedRunTargetTags, error) {
	if len(tags) == 0 {
		return ParsedRunTargetTags{}, fmt.Errorf("run tags are empty")
	}

	selection, namespace, err := parseTargetSelectionFromTags(tags)
	if err != nil {
		return ParsedRunTargetTags{}, err
	}
	backfillKey := strings.TrimSpace(tags[TagDaggoBackfill])
	if backfillKey == "" {
		backfillKey = strings.TrimSpace(tags[TagDagsterBackfill])
	}
	return ParsedRunTargetTags{
		Selection:   selection,
		BackfillKey: backfillKey,
		Namespace:   namespace,
	}, nil
}

func parseTargetSelectionFromTags(tags map[string]string) (NormalizedPartitionSelection, string, error) {
	if key := strings.TrimSpace(tags[TagDaggoPartition]); key != "" {
		return NormalizedPartitionSelection{
			Mode:   PartitionSelectionModeSingle,
			Keys:   []string{key},
			Ranges: []PartitionSelectionRange{{StartKey: key, EndKey: key}},
		}, "daggo", nil
	}
	if key := strings.TrimSpace(tags[TagDagsterPartition]); key != "" {
		return NormalizedPartitionSelection{
			Mode:   PartitionSelectionModeSingle,
			Keys:   []string{key},
			Ranges: []PartitionSelectionRange{{StartKey: key, EndKey: key}},
		}, "dagster", nil
	}

	daggoStart := strings.TrimSpace(tags[TagDaggoPartitionRangeStart])
	daggoEnd := strings.TrimSpace(tags[TagDaggoPartitionRangeEnd])
	if daggoStart != "" || daggoEnd != "" {
		if daggoStart == "" || daggoEnd == "" {
			return NormalizedPartitionSelection{}, "", fmt.Errorf("daggo range tags must include both start and end")
		}
		return NormalizedPartitionSelection{
			Mode: PartitionSelectionModeRange,
			Keys: []string{daggoStart, daggoEnd},
			Ranges: []PartitionSelectionRange{
				{StartKey: daggoStart, EndKey: daggoEnd},
			},
		}, "daggo", nil
	}

	dagsterStart := strings.TrimSpace(tags[TagDagsterPartitionRangeStart])
	dagsterEnd := strings.TrimSpace(tags[TagDagsterPartitionRangeEnd])
	if dagsterStart != "" || dagsterEnd != "" {
		if dagsterStart == "" || dagsterEnd == "" {
			return NormalizedPartitionSelection{}, "", fmt.Errorf("dagster range tags must include both start and end")
		}
		return NormalizedPartitionSelection{
			Mode: PartitionSelectionModeRange,
			Keys: []string{dagsterStart, dagsterEnd},
			Ranges: []PartitionSelectionRange{
				{StartKey: dagsterStart, EndKey: dagsterEnd},
			},
		}, "dagster", nil
	}

	subsetJSON := strings.TrimSpace(tags[TagDaggoPartitionSubsetJSON])
	if subsetJSON != "" {
		var keys []string
		if err := json.Unmarshal([]byte(subsetJSON), &keys); err != nil {
			return NormalizedPartitionSelection{}, "", fmt.Errorf("parse daggo subset tag: %w", err)
		}
		normalizedKeys, err := normalizeOrderedKeys(keys, "partition key")
		if err != nil {
			return NormalizedPartitionSelection{}, "", err
		}
		return NormalizedPartitionSelection{
			Mode:   PartitionSelectionModeSubset,
			Keys:   normalizedKeys,
			Ranges: []PartitionSelectionRange{},
		}, "daggo", nil
	}

	return NormalizedPartitionSelection{}, "", fmt.Errorf("run tags do not include partition targeting tags")
}

type MaterializationPlan struct {
	RunTargets []RunTarget
}

func BuildMaterializationPlan(selection NormalizedPartitionSelection) (MaterializationPlan, error) {
	if len(selection.Keys) == 0 {
		return MaterializationPlan{}, fmt.Errorf("selection is empty")
	}
	targets := make([]RunTarget, 0, len(selection.Keys))
	for _, key := range selection.Keys {
		targets = append(targets, RunTarget{
			Selection: NormalizedPartitionSelection{
				Mode:   PartitionSelectionModeSingle,
				Keys:   []string{key},
				Ranges: []PartitionSelectionRange{{StartKey: key, EndKey: key}},
			},
		})
	}
	return MaterializationPlan{RunTargets: targets}, nil
}

type BackfillPolicyMode string

const (
	BackfillPolicySingleRun BackfillPolicyMode = "single_run"
	BackfillPolicyMultiRun  BackfillPolicyMode = "multi_run"
)

type BackfillPolicy struct {
	Mode                BackfillPolicyMode
	MaxPartitionsPerRun int
}

type BackfillPlan struct {
	Policy     BackfillPolicy
	TotalKeys  int
	RunTargets []RunTarget
}

func BuildBackfillPlan(selection NormalizedPartitionSelection, policy BackfillPolicy) (BackfillPlan, error) {
	if len(selection.Keys) == 0 {
		return BackfillPlan{}, fmt.Errorf("selection is empty")
	}
	if policy.Mode == "" {
		policy.Mode = BackfillPolicyMultiRun
	}

	switch policy.Mode {
	case BackfillPolicySingleRun:
		return BackfillPlan{
			Policy:     policy,
			TotalKeys:  len(selection.Keys),
			RunTargets: []RunTarget{{Selection: selection}},
		}, nil
	case BackfillPolicyMultiRun:
		maxPerRun := policy.MaxPartitionsPerRun
		if maxPerRun <= 0 {
			maxPerRun = 1
		}
		policy.MaxPartitionsPerRun = maxPerRun
		targets := make([]RunTarget, 0)
		for _, selectionRange := range selection.Ranges {
			rangeKeys, err := keysWithinRange(selection.Keys, selectionRange)
			if err != nil {
				return BackfillPlan{}, err
			}
			for chunkStart := 0; chunkStart < len(rangeKeys); chunkStart += maxPerRun {
				chunkEnd := chunkStart + maxPerRun
				if chunkEnd > len(rangeKeys) {
					chunkEnd = len(rangeKeys)
				}
				chunk := rangeKeys[chunkStart:chunkEnd]
				targets = append(targets, RunTarget{Selection: normalizeContiguousKeys(chunk)})
			}
		}
		return BackfillPlan{
			Policy:     policy,
			TotalKeys:  len(selection.Keys),
			RunTargets: targets,
		}, nil
	default:
		return BackfillPlan{}, fmt.Errorf("unsupported backfill policy mode %q", policy.Mode)
	}
}

func keysWithinRange(allKeys []string, selectionRange PartitionSelectionRange) ([]string, error) {
	start := -1
	end := -1
	for index, key := range allKeys {
		if key == selectionRange.StartKey && start == -1 {
			start = index
		}
		if key == selectionRange.EndKey {
			end = index
		}
	}
	if start == -1 || end == -1 || start > end {
		return nil, fmt.Errorf("invalid selection range %q..%q", selectionRange.StartKey, selectionRange.EndKey)
	}
	return cloneStrings(allKeys[start : end+1]), nil
}

func normalizeContiguousKeys(keys []string) NormalizedPartitionSelection {
	if len(keys) == 1 {
		return NormalizedPartitionSelection{
			Mode: PartitionSelectionModeSingle,
			Keys: cloneStrings(keys),
			Ranges: []PartitionSelectionRange{
				{StartKey: keys[0], EndKey: keys[0]},
			},
		}
	}
	return NormalizedPartitionSelection{
		Mode: PartitionSelectionModeRange,
		Keys: cloneStrings(keys),
		Ranges: []PartitionSelectionRange{
			{StartKey: keys[0], EndKey: keys[len(keys)-1]},
		},
	}
}
