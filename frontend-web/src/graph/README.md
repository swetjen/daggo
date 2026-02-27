# Graph Renderer Modules

Coordinate model:

- World coordinates are deterministic, grid-aligned layout positions.
- Camera transform is `{ tx, ty, scale }`.
- Conversion rules:
  - `screen.x = world.x * scale + tx`
  - `screen.y = world.y * scale + ty`

Module map:

- `GraphViewport.ts`: world/screen transforms + fit/zoom helpers.
- `dagLayout.ts`: deterministic DAG layout + anchor helpers.
- `CanvasEdgeLayer.tsx`: canvas edge rendering.
- `NodeLayer.tsx`: HTML node overlay.
- `DagGraphCanvasHtml.tsx`: pan/zoom + drag orchestration.

Implementation guardrail:

- Edge anchors come from node model dimensions, never from DOM measurement.
