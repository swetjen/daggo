# DAGGO Frontend Design System

Operational Precision is the active design system for DAGGO.

Canonical spec:
- `docs/DAGGO_DESIGN_SYSTEM_V2.xml`

## Core Direction
- Dark mode first
- Dense, ops-console layout (20-30% tighter than prior Memoria styling)
- Layered surfaces and explicit borders for hierarchy
- State colors only for runtime/severity states
- Minimal motion (fast hover, accordion, tab indicator)

## Token Families
- Surfaces: `bg.app`, `bg.panel`, `bg.card`, `bg.elevated`, `bg.hover`, `bg.active`
- Borders: `border.subtle`, `border.default`, `border.strong`
- Text: `text.primary`, `text.secondary`, `text.muted`, `text.dim`
- State accents: `state.success`, `state.error`, `state.running`, `state.queued`

## Hard Rules
- No BorderBeam, ProgressiveBlur theatrics, 3D glare, starfield effects, or blur-to-clear page reveals
- No decorative accent colors
- Keep run, graph, timeline, and logs optimized for scanning and deterministic interpretation

## Implementation Note
Frontend styles are implemented in `frontend-web/src/index.css`, with the v2 override block as the source of truth for visual behavior.

Logo assets:
- `frontend-web/src/assets/daggo.png` is the active dark-mode logo.
