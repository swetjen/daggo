package daggo

import (
	"context"
	"fmt"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/queue"
)

func RunDefinitions(ctx context.Context, cfg Config, definitions ...any) error {
	jobRegistry, queueRegistry, err := registriesFromDefinitions(definitions...)
	if err != nil {
		return err
	}
	return runWithDefinitions(ctx, cfg, jobRegistry, queueRegistry)
}

func OpenDefinitions(ctx context.Context, cfg Config, definitions ...any) (*App, error) {
	jobRegistry, queueRegistry, err := registriesFromDefinitions(definitions...)
	if err != nil {
		return nil, err
	}
	return openWithDefinitions(ctx, cfg, jobRegistry, queueRegistry)
}

func registriesFromDefinitions(definitions ...any) (*dag.Registry, *queue.Registry, error) {
	jobRegistry := dag.NewRegistry()
	queueRegistry := queue.NewRegistry()
	seenJobs := make(map[string]bool)
	for _, raw := range definitions {
		switch typed := raw.(type) {
		case dag.JobDefinition:
			if seenJobs[typed.Key] {
				continue
			}
			if err := jobRegistry.Register(typed); err != nil {
				return nil, nil, err
			}
			seenJobs[typed.Key] = true
		case queue.Definition:
			if err := queueRegistry.Register(typed); err != nil {
				return nil, nil, err
			}
			for _, job := range typed.Jobs() {
				if seenJobs[job.Key] {
					continue
				}
				if err := jobRegistry.Register(job); err != nil {
					return nil, nil, err
				}
				seenJobs[job.Key] = true
			}
		default:
			return nil, nil, fmt.Errorf("unsupported runtime definition %T", raw)
		}
	}
	return jobRegistry, queueRegistry, nil
}
