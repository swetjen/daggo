import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { PointerEvent as ReactPointerEvent, WheelEvent as ReactWheelEvent, ReactNode } from "react";
import {
  fitViewportToWorldRect,
  GRAPH_MAX_ZOOM,
  GRAPH_MIN_ZOOM,
  viewportPanFromScreenDelta,
  zoomDeltaToScaleFactor,
  zoomViewportAtScreenPoint,
} from "./GraphViewport";
import { CanvasEdgeLayer } from "./CanvasEdgeLayer";
import { DAG_NODE_HARD_CAP } from "./dagLayout";
import { NodeLayer } from "./NodeLayer";
import type { DagEdgeModel, DagNodeModel, GraphViewport } from "./types";

type DagGraphCanvasHtmlProps = {
  layoutKey: string;
  themeMode?: "dark" | "light";
  worldWidth: number;
  worldHeight: number;
  nodes: DagNodeModel[];
  edges: DagEdgeModel[];
  dependenciesByKey: Record<string, string[]>;
  dependentsByKey: Record<string, string[]>;
  selectedNodeId?: string;
  getNodeClassName: (node: DagNodeModel) => string;
  renderNodeContent: (node: DagNodeModel) => ReactNode;
  onNodeClick?: (node: DagNodeModel) => void;
  onNodeMouseEnter?: (node: DagNodeModel) => void;
  onNodeMouseLeave?: (node: DagNodeModel) => void;
  emptyMessage?: string;
  totalNodeCount: number;
  hardCap?: number;
};

type SurfaceSize = {
  width: number;
  height: number;
};

type PanState = {
  pointerId: number;
  startClientX: number;
  startClientY: number;
  startTx: number;
  startTy: number;
};

export function DagGraphCanvasHtml(props: DagGraphCanvasHtmlProps) {
  const {
    layoutKey,
    themeMode = "dark",
    worldWidth,
    worldHeight,
    nodes,
    edges,
    dependenciesByKey,
    dependentsByKey,
    selectedNodeId = "",
    getNodeClassName,
    renderNodeContent,
    onNodeClick,
    onNodeMouseEnter,
    onNodeMouseLeave,
    emptyMessage = "No steps match this filter.",
    totalNodeCount,
    hardCap = DAG_NODE_HARD_CAP,
  } = props;

  const surfaceRef = useRef<HTMLDivElement | null>(null);
  const [surfaceSize, setSurfaceSize] = useState<SurfaceSize>({ width: 0, height: 0 });
  const [viewport, setViewport] = useState<GraphViewport>({ tx: 0, ty: 0, scale: 1 });
  const [draggedNodePositionById, setDraggedNodePositionById] = useState<Record<string, { x: number; y: number }>>({});

  const panStateRef = useRef<PanState | null>(null);

  useEffect(() => {
    const surface = surfaceRef.current;
    if (!surface) {
      return;
    }

    const measure = () => {
      setSurfaceSize({
        width: surface.clientWidth,
        height: surface.clientHeight,
      });
    };

    measure();
    let observer: ResizeObserver | null = null;
    if (typeof ResizeObserver !== "undefined") {
      observer = new ResizeObserver(measure);
      observer.observe(surface);
    }
    window.addEventListener("resize", measure);

    return () => {
      window.removeEventListener("resize", measure);
      observer?.disconnect();
    };
  }, []);

  useEffect(() => {
    const initial: Record<string, { x: number; y: number }> = {};
    for (const node of nodes) {
      initial[node.id] = { x: node.x, y: node.y };
    }
    setDraggedNodePositionById(initial);
  }, [layoutKey]);

  useEffect(() => {
    setDraggedNodePositionById((previous) => {
      const next: Record<string, { x: number; y: number }> = {};
      for (const node of nodes) {
        next[node.id] = previous[node.id] ?? { x: node.x, y: node.y };
      }
      return next;
    });
  }, [nodes]);

  const fitToGraph = useCallback(() => {
    if (surfaceSize.width <= 0 || surfaceSize.height <= 0) {
      return;
    }
    setViewport(
      fitViewportToWorldRect({
        worldRect: {
          x: 0,
          y: 0,
          width: worldWidth,
          height: worldHeight,
        },
        screenWidth: surfaceSize.width,
        screenHeight: surfaceSize.height,
        minZoom: GRAPH_MIN_ZOOM,
        maxZoom: GRAPH_MAX_ZOOM,
      }),
    );
  }, [surfaceSize.height, surfaceSize.width, worldHeight, worldWidth]);

  useEffect(() => {
    fitToGraph();
  }, [fitToGraph, layoutKey]);

  const positionedNodes = useMemo(
    () =>
      nodes.map((node) => {
        const override = draggedNodePositionById[node.id];
        if (!override) {
          return node;
        }
        return {
          ...node,
          x: override.x,
          y: override.y,
        };
      }),
    [nodes, draggedNodePositionById],
  );

  const surfacePointFromClient = useCallback((clientX: number, clientY: number) => {
    const surface = surfaceRef.current;
    if (!surface) {
      return { x: clientX, y: clientY };
    }
    const rect = surface.getBoundingClientRect();
    return {
      x: clientX - rect.left,
      y: clientY - rect.top,
    };
  }, []);

  const handleSurfacePointerDown = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      if (event.button !== 0) {
        return;
      }
      if ((event.target as HTMLElement).closest(".dag-screen-node")) {
        return;
      }
      const surface = surfaceRef.current;
      if (!surface) {
        return;
      }
      surface.setPointerCapture(event.pointerId);
      panStateRef.current = {
        pointerId: event.pointerId,
        startClientX: event.clientX,
        startClientY: event.clientY,
        startTx: viewport.tx,
        startTy: viewport.ty,
      };
    },
    [viewport.tx, viewport.ty],
  );

  const handleNodePointerDown = useCallback((event: ReactPointerEvent<HTMLButtonElement>) => {
    if (event.button !== 0) {
      return;
    }
    event.stopPropagation();
  }, []);

  const handlePointerMove = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      const panState = panStateRef.current;
      if (panState && panState.pointerId === event.pointerId) {
        const dx = event.clientX - panState.startClientX;
        const dy = event.clientY - panState.startClientY;
        setViewport({
          tx: panState.startTx + dx,
          ty: panState.startTy + dy,
          scale: viewport.scale,
        });
      }
    },
    [viewport.scale],
  );

  const clearPointerState = useCallback((pointerId: number) => {
    const surface = surfaceRef.current;
    if (surface && surface.hasPointerCapture(pointerId)) {
      surface.releasePointerCapture(pointerId);
    }

    const panState = panStateRef.current;
    if (panState?.pointerId === pointerId) {
      panStateRef.current = null;
    }
  }, []);

  const handleNodeClick = useCallback((node: DagNodeModel) => onNodeClick?.(node), [onNodeClick]);

  const handleSurfaceWheel = useCallback(
    (event: ReactWheelEvent<HTMLDivElement>) => {
      event.preventDefault();
      event.stopPropagation();
      if (surfaceSize.width <= 0 || surfaceSize.height <= 0) {
        return;
      }
      const wantsZoom = event.ctrlKey || event.altKey || event.metaKey;
      if (wantsZoom) {
        const local = surfacePointFromClient(event.clientX, event.clientY);
        const factor = zoomDeltaToScaleFactor(event.deltaY);
        setViewport((current) =>
          zoomViewportAtScreenPoint({
            viewport: current,
            nextScale: current.scale * factor,
            screenX: local.x,
            screenY: local.y,
            minZoom: GRAPH_MIN_ZOOM,
            maxZoom: GRAPH_MAX_ZOOM,
          }),
        );
        return;
      }
      setViewport((current) => viewportPanFromScreenDelta(current, -event.deltaX, -event.deltaY));
    },
    [surfacePointFromClient, surfaceSize.height, surfaceSize.width],
  );

  const handleWheelCapture = useCallback((event: ReactWheelEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
  }, []);

  const panActive = panStateRef.current !== null;

  if (totalNodeCount > hardCap) {
    return (
      <div className="dag-fallback-state" role="status" aria-live="polite">
        <h4>Graph Too Large To Render</h4>
        <p>
          This graph has <strong>{totalNodeCount}</strong> nodes, above the <strong>{hardCap}</strong> node visualization cap.
        </p>
        <p>Try one of these actions:</p>
        <ul>
          <li>Filter to a subset of steps.</li>
          <li>Partition by domain or owner.</li>
          <li>Drill into a narrower run or job slice.</li>
        </ul>
      </div>
    );
  }

  return (
    <div className="dag-canvas-html-root" onWheelCapture={handleWheelCapture}>
      <div className="dag-camera-controls">
        <button
          className="ghost-btn tiny"
          onClick={() =>
            setViewport((current) =>
              zoomViewportAtScreenPoint({
                viewport: current,
                nextScale: current.scale * 1.14,
                screenX: surfaceSize.width / 2,
                screenY: surfaceSize.height / 2,
              }),
            )
          }
        >
          Zoom +
        </button>
        <button
          className="ghost-btn tiny"
          onClick={() =>
            setViewport((current) =>
              zoomViewportAtScreenPoint({
                viewport: current,
                nextScale: current.scale / 1.14,
                screenX: surfaceSize.width / 2,
                screenY: surfaceSize.height / 2,
              }),
            )
          }
        >
          Zoom -
        </button>
        <button className="ghost-btn tiny" onClick={fitToGraph}>
          Reset View
        </button>
        <span className="dag-zoom-indicator">{Math.round(viewport.scale * 100)}%</span>
      </div>

      <div
        ref={surfaceRef}
        className={`dag-canvas-html-surface ${panActive ? "panning" : ""}`}
        onPointerDown={handleSurfacePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={(event) => clearPointerState(event.pointerId)}
        onPointerCancel={(event) => clearPointerState(event.pointerId)}
        onWheel={handleSurfaceWheel}
      >
        <CanvasEdgeLayer
          themeMode={themeMode}
          width={surfaceSize.width}
          height={surfaceSize.height}
          viewport={viewport}
          nodes={positionedNodes}
          edges={edges}
          dependenciesByKey={dependenciesByKey}
          dependentsByKey={dependentsByKey}
          selectedNodeId={selectedNodeId}
        />

        <NodeLayer
          nodes={positionedNodes}
          viewport={viewport}
          getNodeClassName={getNodeClassName}
          renderNodeContent={renderNodeContent}
          onNodeClick={handleNodeClick}
          onNodePointerDown={handleNodePointerDown}
          onNodeMouseEnter={onNodeMouseEnter}
          onNodeMouseLeave={onNodeMouseLeave}
        />

        {nodes.length === 0 ? <p className="run-graph-empty">{emptyMessage}</p> : null}
      </div>
    </div>
  );
}
