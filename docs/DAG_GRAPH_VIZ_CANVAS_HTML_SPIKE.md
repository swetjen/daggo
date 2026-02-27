# DAGGO Graph Viz Spike: Canvas Edges + HTML Nodes

Date: 2026-02-26
Status: Experimental spike plan

## Decision Summary

Adopt a hybrid renderer for DAG views:

- Canvas layer for edges and grid.
- HTML layer for nodes.
- Single world coordinate system for both.
- Camera transform (`tx`, `ty`, `scale`) for pan/zoom.

This spike directly targets the current instability class (misattached edges, anchor drift, transform mismatches) by removing DOM measurement from edge attachment and making node geometry deterministic in the data model.

## Why This Fits DAGGO

### Correctness and Stability

Pros:
- One coordinate source of truth (world space).
- No `getBoundingClientRect` dependency for edge anchors.
- Edge geometry is deterministic from node model and dependency slots.
- Pan and zoom are purely camera operations, so edges/nodes cannot diverge.

Cons:
- Requires careful viewport/event logic to avoid interaction glitches.
- CSS hover transforms on nodes must not break scale assumptions (handled by scaled wrappers).

Net: Strong improvement over DOM-measured SVG paths for DAGGO’s observed bugs.

### Interaction Effort

Pros:
- Node interactions remain easy in HTML (hover, click, popover, focus states).
- Layout-only drag is straightforward in world coordinates.
- Keyboard/accessibility behavior stays in familiar DOM controls.

Cons:
- Need explicit gesture arbitration (pan vs node drag vs click suppression).
- Zoom/pan UX needs product tuning (bounds, inertia, hotkeys).

Net: Moderate effort, but isolated to frontend graph surface logic.

### Performance Characteristics (<= 500 Nodes)

Pros:
- Single canvas draw call path is efficient for hundreds of edges.
- HTML node count at this scale is manageable on high-end machines.
- No resize observer loop for edge recalculation based on measured DOM bounds.

Cons:
- Node DOM is still non-trivial at upper bound; avoid expensive per-node effects.
- Canvas redraw cost scales with edge count and zoom/pan updates.

Net: Acceptable for DAGGO’s explicit hard cap; architecture aligns with the constraint.

### Maintainability

Pros:
- Clear module boundaries:
  - `GraphViewport` for transform math.
  - `dagLayout` for deterministic graph placement.
  - `CanvasEdgeLayer` for edge rendering.
  - `NodeLayer`/`DagGraphCanvasHtml` for HTML nodes + interaction.
- Easier to unit-test geometry and layout without DOM.

Cons:
- More explicit rendering infrastructure than inline SVG/DOM approach.
- Requires discipline to keep all geometry in world space.

Net: Better long-term than the previous mixed DOM-measurement path.

## Future Risks

### If We Later Need Collapsible Groups

Risk:
- Group collapse introduces dynamic geometry changes and partial visibility rules.

Impact:
- Edge routing and slot assignment must account for hidden/internal nodes.
- Group container anchors may become first-class anchor targets.

Mitigation:
- Keep layout and edge generation as pure functions over visible graph state.
- Introduce group nodes as explicit world entities instead of special-case DOM logic.

### If We Later Need Editing Features

Risk:
- Interactive rewiring, insertion, and reorder constraints require richer hit-testing and previews.

Impact:
- Need robust edge hit areas, drag handles, snapping guides, and mutation UX.

Mitigation:
- Preserve current world/camera abstraction so editing tools can build on it.
- Add explicit edit state machine (selection, drag edge, preview, commit) instead of ad hoc handlers.

## Spike Plan (Implementation)

1. Coordinate Systems
- Done in spike module: world-space + camera transforms + fit-to-bounds + zoom-at-cursor.

2. Deterministic Node Model and Anchors
- Done: fixed-size presets and anchor functions derived from model.
- Done: layout-only drag updates node world position only.

3. Canvas Edge Renderer
- Done: single canvas with cubic Bezier edges, tangent-aligned arrowheads, deterministic fan-out offsets.

4. HTML Node Layer
- Done: absolute-positioned DOM nodes with true zoom (option A) via scaled wrappers.

5. Layout Strategy (Grid-Fixed)
- Done: depth columns + barycentric row ordering heuristic + stable sorting.

6. Hard Cap + Fallback
- Done: cap enforced at 500 nodes with product-facing fallback guidance.

7. Verification Targets
- Confirm no edge drift under pan/zoom.
- Confirm layout-only dragging stability.
- Confirm readability and frame behavior near upper bound.

## Go/No-Go Criteria for Adoption

Go if:
- No observable edge/node drift under aggressive pan/zoom/drag.
- Interaction behavior is stable on job and run detail pages.
- Performance is smooth on representative 300-500 node graphs.

No-go if:
- Gesture conflicts significantly degrade usability.
- Rendering or interaction regressions exceed acceptable maintenance cost.
