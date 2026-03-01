# Job Definitions Simplification TODO

1. Split large jobs into stage-focused files so DAG wiring is separate from domain utilities and external integration logic.
2. Add helpers for repeated node declaration boilerplate (`dag.Op(...).WithDisplayName(...).WithDescription(...)`) and schedule boilerplate.
3. Replace map-based runtime parameter parsing with one typed config decode path.
4. Convert raw integer status constants into typed enums plus shared status metadata maps.
5. Standardize job logging on one structured path (avoid mixed logger and raw `fmt.Printf` patterns).
6. Move large static filtering/rule lists out of core job logic into dedicated data modules.
7. Inject external dependencies (storage, browser automation, model clients) into step execution context rather than constructing inside step functions.
8. Centralize persistence responsibilities per job domain instead of scattering writes across many stage functions.
9. Introduce reusable synthetic-step helpers for demo/test jobs with repeated sleep/output patterns.
10. Simplify registry wiring to register jobs from a single list/loop for easier additions/removals.
