package dag

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type UpstreamMappedPartitionsResult struct {
	PartitionKeys              []string
	RequiredButNonexistentKeys []string
}

type PartitionMapping interface {
	MapUpstreamPartitionKeys(
		ctx context.Context,
		downstreamPartitionKeys []string,
		downstreamDefinition PartitionDefinition,
		upstreamDefinition PartitionDefinition,
		now time.Time,
	) ([]string, error)
}

func ResolveUpstreamMappedPartitionsForSelection(
	ctx context.Context,
	mapping PartitionMapping,
	selection NormalizedPartitionSelection,
	downstreamDefinition PartitionDefinition,
	upstreamDefinition PartitionDefinition,
	now time.Time,
) (UpstreamMappedPartitionsResult, error) {
	if downstreamDefinition == nil {
		return UpstreamMappedPartitionsResult{}, fmt.Errorf("downstream partition definition is required")
	}
	if upstreamDefinition == nil {
		return UpstreamMappedPartitionsResult{}, fmt.Errorf("upstream partition definition is required")
	}
	if len(selection.Keys) == 0 {
		return UpstreamMappedPartitionsResult{}, fmt.Errorf("downstream selection is empty")
	}

	if mapping == nil {
		mapping = IdentityPartitionMapping{}
	}

	requiredKeys, err := mapping.MapUpstreamPartitionKeys(
		ctx,
		selection.Keys,
		downstreamDefinition,
		upstreamDefinition,
		now,
	)
	if err != nil {
		return UpstreamMappedPartitionsResult{}, err
	}
	if len(requiredKeys) == 0 {
		return UpstreamMappedPartitionsResult{
			PartitionKeys:              []string{},
			RequiredButNonexistentKeys: []string{},
		}, nil
	}
	requiredKeys, err = normalizeOrderedKeys(requiredKeys, "partition key")
	if err != nil {
		return UpstreamMappedPartitionsResult{}, err
	}

	upstreamExistingKeys, err := upstreamDefinition.PartitionKeys(ctx, now)
	if err != nil {
		return UpstreamMappedPartitionsResult{}, err
	}
	upstreamExistingSet := make(map[string]struct{}, len(upstreamExistingKeys))
	for _, key := range upstreamExistingKeys {
		upstreamExistingSet[key] = struct{}{}
	}

	existent := make([]string, 0, len(requiredKeys))
	missing := make([]string, 0)
	for _, key := range requiredKeys {
		if _, exists := upstreamExistingSet[key]; exists {
			existent = append(existent, key)
		} else {
			missing = append(missing, key)
		}
	}
	return UpstreamMappedPartitionsResult{
		PartitionKeys:              existent,
		RequiredButNonexistentKeys: missing,
	}, nil
}

type IdentityPartitionMapping struct{}

func (IdentityPartitionMapping) MapUpstreamPartitionKeys(
	_ context.Context,
	downstreamPartitionKeys []string,
	_ PartitionDefinition,
	_ PartitionDefinition,
	_ time.Time,
) ([]string, error) {
	return normalizeOrderedKeys(downstreamPartitionKeys, "partition key")
}

type StaticPartitionMapping struct {
	DownstreamToUpstream map[string][]string
}

func (m StaticPartitionMapping) MapUpstreamPartitionKeys(
	_ context.Context,
	downstreamPartitionKeys []string,
	_ PartitionDefinition,
	_ PartitionDefinition,
	_ time.Time,
) ([]string, error) {
	keys, err := normalizeOrderedKeys(downstreamPartitionKeys, "partition key")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, downstreamKey := range keys {
		mapped := m.DownstreamToUpstream[downstreamKey]
		for _, mappedKey := range mapped {
			key := strings.TrimSpace(mappedKey)
			if key == "" {
				return nil, fmt.Errorf("static partition mapping for %q contains empty upstream key", downstreamKey)
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, key)
		}
	}
	return out, nil
}

type AllPartitionMapping struct{}

func (AllPartitionMapping) MapUpstreamPartitionKeys(
	ctx context.Context,
	_ []string,
	_ PartitionDefinition,
	upstreamDefinition PartitionDefinition,
	now time.Time,
) ([]string, error) {
	return upstreamDefinition.PartitionKeys(ctx, now)
}

type LastPartitionMapping struct{}

func (LastPartitionMapping) MapUpstreamPartitionKeys(
	ctx context.Context,
	_ []string,
	_ PartitionDefinition,
	upstreamDefinition PartitionDefinition,
	now time.Time,
) ([]string, error) {
	upstreamKeys, err := upstreamDefinition.PartitionKeys(ctx, now)
	if err != nil {
		return nil, err
	}
	if len(upstreamKeys) == 0 {
		return nil, fmt.Errorf("upstream partition definition has no keys")
	}
	return []string{upstreamKeys[len(upstreamKeys)-1]}, nil
}

type TimeWindowPartitionMapping struct {
	StartOffset int
	EndOffset   int
}

func (m TimeWindowPartitionMapping) MapUpstreamPartitionKeys(
	ctx context.Context,
	downstreamPartitionKeys []string,
	downstreamDefinition PartitionDefinition,
	upstreamDefinition PartitionDefinition,
	now time.Time,
) ([]string, error) {
	downstreamTimeDef, ok := downstreamDefinition.(TimeWindowPartitionDefinition)
	if !ok {
		return nil, fmt.Errorf("downstream definition must be TimeWindowPartitionDefinition")
	}
	upstreamTimeDef, ok := upstreamDefinition.(TimeWindowPartitionDefinition)
	if !ok {
		return nil, fmt.Errorf("upstream definition must be TimeWindowPartitionDefinition")
	}

	downstreamKeys, err := normalizeOrderedKeys(downstreamPartitionKeys, "partition key")
	if err != nil {
		return nil, err
	}
	upstreamStep, err := upstreamTimeDef.stepDuration()
	if err != nil {
		return nil, err
	}
	downstreamStep, err := downstreamTimeDef.stepDuration()
	if err != nil {
		return nil, err
	}

	location := downstreamTimeDef.Location
	if location == nil {
		location = time.UTC
	}
	upstreamFormat := strings.TrimSpace(upstreamTimeDef.Format)
	if upstreamFormat == "" {
		upstreamFormat = upstreamTimeDef.defaultFormat()
	}

	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, downstreamKey := range downstreamKeys {
		downstreamStart, parseErr := parseTimeWindowPartitionKey(downstreamKey, downstreamTimeDef)
		if parseErr != nil {
			return nil, parseErr
		}
		downstreamEnd := downstreamStart.Add(downstreamStep)

		searchStart := downstreamStart.Add(time.Duration(m.StartOffset) * upstreamStep)
		searchEnd := downstreamEnd.Add(time.Duration(m.EndOffset) * upstreamStep)
		if searchEnd.Before(searchStart) {
			searchStart, searchEnd = searchEnd, searchStart
		}

		cursor := truncateToCadence(searchStart, upstreamTimeDef.Cadence)
		for cursor.Before(searchEnd) {
			windowEnd := cursor.Add(upstreamStep)
			if windowEnd.After(searchStart) && cursor.Before(searchEnd) {
				key := cursor.In(location).Format(upstreamFormat)
				if _, exists := seen[key]; !exists {
					seen[key] = struct{}{}
					out = append(out, key)
				}
			}
			cursor = cursor.Add(upstreamStep)
		}
	}
	return out, nil
}

func parseTimeWindowPartitionKey(key string, definition TimeWindowPartitionDefinition) (time.Time, error) {
	location := definition.Location
	if location == nil {
		location = time.UTC
	}
	format := strings.TrimSpace(definition.Format)
	if format == "" {
		format = definition.defaultFormat()
	}
	parsed, err := time.ParseInLocation(format, key, location)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time window partition key %q: %w", key, err)
	}
	return truncateToCadence(parsed, definition.Cadence), nil
}

type MultiPartitionDimensionMapping struct {
	UpstreamDimension   string
	DownstreamDimension string
	Mapping             PartitionMapping
}

type MultiPartitionMapping struct {
	DimensionMappings []MultiPartitionDimensionMapping
}

func (m MultiPartitionMapping) MapUpstreamPartitionKeys(
	ctx context.Context,
	downstreamPartitionKeys []string,
	downstreamDefinition PartitionDefinition,
	upstreamDefinition PartitionDefinition,
	now time.Time,
) ([]string, error) {
	downstreamMulti, ok := downstreamDefinition.(*MultiPartitionDefinition)
	if !ok {
		return nil, fmt.Errorf("downstream definition must be MultiPartitionDefinition")
	}
	upstreamMulti, ok := upstreamDefinition.(*MultiPartitionDefinition)
	if !ok {
		return nil, fmt.Errorf("upstream definition must be MultiPartitionDefinition")
	}

	normalizedDownstreamKeys, err := normalizeOrderedKeys(downstreamPartitionKeys, "partition key")
	if err != nil {
		return nil, err
	}
	downstreamNames := downstreamMulti.DimensionNames()
	upstreamNames := upstreamMulti.DimensionNames()

	mappingByUpstreamDimension := make(map[string]MultiPartitionDimensionMapping, len(m.DimensionMappings))
	for _, mapping := range m.DimensionMappings {
		upstreamDimension := strings.TrimSpace(mapping.UpstreamDimension)
		downstreamDimension := strings.TrimSpace(mapping.DownstreamDimension)
		if upstreamDimension == "" || downstreamDimension == "" {
			return nil, fmt.Errorf("multi partition dimension mapping requires upstream and downstream dimensions")
		}
		if _, exists := upstreamMulti.dimensions[upstreamDimension]; !exists {
			return nil, fmt.Errorf("unknown upstream dimension %q", upstreamDimension)
		}
		if _, exists := downstreamMulti.dimensions[downstreamDimension]; !exists {
			return nil, fmt.Errorf("unknown downstream dimension %q", downstreamDimension)
		}
		if _, exists := mappingByUpstreamDimension[upstreamDimension]; exists {
			return nil, fmt.Errorf("duplicate mapping for upstream dimension %q", upstreamDimension)
		}
		mappingByUpstreamDimension[upstreamDimension] = mapping
	}

	upstreamDimensionKeys := make(map[string][]string, len(upstreamNames))
	for _, upstreamDimension := range upstreamNames {
		definition := upstreamMulti.dimensions[upstreamDimension]
		keys, keyErr := definition.PartitionKeys(ctx, now)
		if keyErr != nil {
			return nil, keyErr
		}
		upstreamDimensionKeys[upstreamDimension] = keys
	}

	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, downstreamKey := range normalizedDownstreamKeys {
		downstreamParts, parseErr := parseMultiPartitionKey(downstreamKey, downstreamNames)
		if parseErr != nil {
			return nil, parseErr
		}

		upstreamDimensionCandidates := make(map[string][]string, len(upstreamNames))
		for _, upstreamDimension := range upstreamNames {
			if mapping, exists := mappingByUpstreamDimension[upstreamDimension]; exists {
				downstreamDimensionDefinition := downstreamMulti.dimensions[mapping.DownstreamDimension]
				upstreamDimensionDefinition := upstreamMulti.dimensions[upstreamDimension]
				mappingToUse := mapping.Mapping
				if mappingToUse == nil {
					mappingToUse = IdentityPartitionMapping{}
				}
				mappedKeys, mapErr := mappingToUse.MapUpstreamPartitionKeys(
					ctx,
					[]string{downstreamParts[mapping.DownstreamDimension]},
					downstreamDimensionDefinition,
					upstreamDimensionDefinition,
					now,
				)
				if mapErr != nil {
					return nil, mapErr
				}
				upstreamDimensionCandidates[upstreamDimension] = mappedKeys
				continue
			}

			if downstreamValue, exists := downstreamParts[upstreamDimension]; exists {
				upstreamDimensionCandidates[upstreamDimension] = []string{downstreamValue}
				continue
			}
			upstreamDimensionCandidates[upstreamDimension] = cloneStrings(upstreamDimensionKeys[upstreamDimension])
		}

		expanded := crossProductMultiPartitionKeys(upstreamNames, upstreamDimensionCandidates)
		for _, candidate := range expanded {
			if _, exists := seen[candidate]; exists {
				continue
			}
			seen[candidate] = struct{}{}
			out = append(out, candidate)
		}
	}
	return out, nil
}

func parseMultiPartitionKey(key string, dimensionNames []string) (map[string]string, error) {
	parts := strings.Split(key, "|")
	if len(parts) != len(dimensionNames) {
		return nil, fmt.Errorf("multi partition key %q has %d dimensions, expected %d", key, len(parts), len(dimensionNames))
	}
	out := make(map[string]string, len(parts))
	for index, dimensionName := range dimensionNames {
		value := strings.TrimSpace(parts[index])
		if value == "" {
			return nil, fmt.Errorf("multi partition key %q has empty value for dimension %q", key, dimensionName)
		}
		out[dimensionName] = value
	}
	return out, nil
}

func crossProductMultiPartitionKeys(dimensionNames []string, candidates map[string][]string) []string {
	if len(dimensionNames) == 0 {
		return nil
	}
	working := make([]string, 0)
	for _, name := range dimensionNames {
		keys := candidates[name]
		sort.Strings(keys)
		if len(working) == 0 {
			for _, key := range keys {
				working = append(working, key)
			}
			continue
		}
		next := make([]string, 0, len(working)*len(keys))
		for _, prefix := range working {
			for _, key := range keys {
				next = append(next, prefix+"|"+key)
			}
		}
		working = next
	}
	return working
}
