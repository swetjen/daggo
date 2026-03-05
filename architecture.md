## 12. Extensibility Boundaries (Future-Safe)

### Planned (not implemented)
- WASM Nodes
- External executors
- Distributed workers
- Object storage plugins

### Architectural hooks
- Node interface abstraction
- Executor interface
- Output materialization abstraction
- Event schema stability

**Design rule**: future extensions must plug in without weakening type safety.

---

## 13. Architectural Invariants (Non-Negotiable)
- Typed inputs/outputs everywhere.
- No string-based wiring.
- No runtime DAG mutation.
- No hidden magic.
- Fail early.
- Event log is truth.

---

## 14. Why This Architecture Works
- Go-native: leans into `context`, goroutines, generics.
- Simple first: v1 is understandable in a week.
- Dagster-inspired, not Dagster-copied.
- UI-ready from day one.
- Scales conceptually without over-engineering.
