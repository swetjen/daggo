package jobs

import "daggo/dag"

func DefaultRegistry() *dag.Registry {
	registry := dag.NewRegistry()
	// Intentionally empty in the standalone DAGGO repo.
	// Add implementation-specific jobs via registry.MustRegister(...).
	return registry
}
