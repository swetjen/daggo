import { useEffect, useRef } from "react";
import { screenToWorld, worldToScreen } from "./GraphViewport";
import { centeredSlotOffset, incomingAnchor, outgoingAnchor, WORLD_GRID_UNIT } from "./dagLayout";
import type { DagEdgeModel, DagNodeModel, GraphViewport, WorldPoint } from "./types";

type CanvasEdgeLayerProps = {
  themeMode?: "dark" | "light";
  width: number;
  height: number;
  viewport: GraphViewport;
  nodes: DagNodeModel[];
  edges: DagEdgeModel[];
  dependenciesByKey: Record<string, string[]>;
  dependentsByKey: Record<string, string[]>;
  selectedNodeId: string;
  showGrid?: boolean;
};

export function CanvasEdgeLayer(props: CanvasEdgeLayerProps) {
  const {
    themeMode = "dark",
    width,
    height,
    viewport,
    nodes,
    edges,
    dependenciesByKey,
    dependentsByKey,
    selectedNodeId,
    showGrid = true,
  } = props;
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const edgeColor = themeMode === "light" ? "#cbd5e1" : "#344055";
  const edgeHighlightColor = themeMode === "light" ? "#2563eb" : "#60789a";
  const gridLineMajorColor = themeMode === "light" ? "rgba(232, 236, 241, 0.9)" : "rgba(52, 64, 85, 0.24)";
  const gridLineMinorColor = themeMode === "light" ? "rgba(232, 236, 241, 0.6)" : "rgba(52, 64, 85, 0.12)";

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || width <= 0 || height <= 0) {
      return;
    }

    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.max(1, Math.floor(width * dpr));
    canvas.height = Math.max(1, Math.floor(height * dpr));
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;

    const context = canvas.getContext("2d");
    if (!context) {
      return;
    }

    context.setTransform(dpr, 0, 0, dpr, 0, 0);
    context.clearRect(0, 0, width, height);

    if (showGrid) {
      drawGrid(context, width, height, viewport, {
        major: gridLineMajorColor,
        minor: gridLineMinorColor,
      });
    }

    const nodeById = new Map(nodes.map((node) => [node.id, node]));
    for (const edge of edges) {
      const source = nodeById.get(edge.from);
      const target = nodeById.get(edge.to);
      if (!source || !target) {
        continue;
      }

      const outbound = dependentsByKey[edge.from] ?? [];
      const inbound = dependenciesByKey[edge.to] ?? [];
      const sourceSlot = outbound.indexOf(edge.to);
      const targetSlot = inbound.indexOf(edge.from);

      const sourceAnchor = outgoingAnchor(source);
      const targetAnchor = incomingAnchor(target);
      const sourceY = clamp(
        sourceAnchor.y + centeredSlotOffset(sourceSlot, outbound.length, 12),
        source.y + 8,
        source.y + source.height - 8,
      );
      const targetY = clamp(
        targetAnchor.y + centeredSlotOffset(targetSlot, inbound.length, 12),
        target.y + 8,
        target.y + target.height - 8,
      );

      const startWorld: WorldPoint = {
        x: sourceAnchor.x,
        y: sourceY,
      };
      const anchorWorld: WorldPoint = {
        x: targetAnchor.x,
        y: targetY,
      };

      const controlsWorld = buildBezierControlsWorld(startWorld, anchorWorld, edge.backEdge);
      const tangentWorld = normalizeVector({
        x: anchorWorld.x - controlsWorld.c2.x,
        y: anchorWorld.y - controlsWorld.c2.y,
      });
      const arrowInsetWorld = 9 / Math.max(0.001, viewport.scale);
      const endWorld: WorldPoint = {
        x: anchorWorld.x - tangentWorld.x * arrowInsetWorld,
        y: anchorWorld.y - tangentWorld.y * arrowInsetWorld,
      };

      const start = worldToScreen(viewport, startWorld);
      const control1 = worldToScreen(viewport, controlsWorld.c1);
      const control2 = worldToScreen(viewport, controlsWorld.c2);
      const end = worldToScreen(viewport, endWorld);

      const highlighted = !selectedNodeId || edge.from === selectedNodeId || edge.to === selectedNodeId;
      const stroke = highlighted ? edgeHighlightColor : edgeColor;

      context.beginPath();
      context.moveTo(start.x, start.y);
      context.bezierCurveTo(control1.x, control1.y, control2.x, control2.y, end.x, end.y);
      context.strokeStyle = stroke;
      context.globalAlpha = highlighted ? 0.78 : 0.38;
      context.lineWidth = highlighted ? 1.9 : 1.7;
      context.lineCap = "round";
      context.lineJoin = "round";
      context.setLineDash(edge.backEdge ? [4, 4] : []);
      context.stroke();

      context.setLineDash([]);
      context.globalAlpha = highlighted ? 0.8 : 0.45;
      drawArrowhead(context, {
        tip: end,
        tangent: normalizeVector({
          x: end.x - control2.x,
          y: end.y - control2.y,
        }),
        stroke,
      });
    }

    context.globalAlpha = 1;
  }, [
    dependenciesByKey,
    dependentsByKey,
    edgeColor,
    edgeHighlightColor,
    edges,
    gridLineMajorColor,
    gridLineMinorColor,
    height,
    nodes,
    selectedNodeId,
    showGrid,
    viewport,
    width,
  ]);

  return <canvas ref={canvasRef} className="dag-canvas-edge-layer" aria-hidden="true" />;
}

function drawGrid(
  context: CanvasRenderingContext2D,
  width: number,
  height: number,
  viewport: GraphViewport,
  colors: { major: string; minor: string },
): void {
  const topLeft = screenToWorld(viewport, { x: 0, y: 0 });
  const bottomRight = screenToWorld(viewport, { x: width, y: height });

  const minX = Math.min(topLeft.x, bottomRight.x);
  const maxX = Math.max(topLeft.x, bottomRight.x);
  const minY = Math.min(topLeft.y, bottomRight.y);
  const maxY = Math.max(topLeft.y, bottomRight.y);

  const step = WORLD_GRID_UNIT;

  const firstX = Math.floor(minX / step) * step;
  const firstY = Math.floor(minY / step) * step;

  context.save();
  context.lineWidth = 1;

  for (let x = firstX; x <= maxX + step; x += step) {
    const screen = worldToScreen(viewport, { x, y: 0 });
    const major = Math.round(x / step) % 4 === 0;
    context.beginPath();
    context.moveTo(Math.round(screen.x) + 0.5, 0);
    context.lineTo(Math.round(screen.x) + 0.5, height);
    context.strokeStyle = major ? colors.major : colors.minor;
    context.stroke();
  }

  for (let y = firstY; y <= maxY + step; y += step) {
    const screen = worldToScreen(viewport, { x: 0, y });
    const major = Math.round(y / step) % 4 === 0;
    context.beginPath();
    context.moveTo(0, Math.round(screen.y) + 0.5);
    context.lineTo(width, Math.round(screen.y) + 0.5);
    context.strokeStyle = major ? colors.major : colors.minor;
    context.stroke();
  }

  context.restore();
}

function drawArrowhead(
  context: CanvasRenderingContext2D,
  options: {
    tip: WorldPoint;
    tangent: WorldPoint;
    stroke: string;
  },
): void {
  const { tip, tangent, stroke } = options;
  const length = 6.8;
  const width = 3.1;
  const baseX = tip.x - tangent.x * length;
  const baseY = tip.y - tangent.y * length;
  const normalX = -tangent.y;
  const normalY = tangent.x;

  const leftX = baseX + normalX * width;
  const leftY = baseY + normalY * width;
  const rightX = baseX - normalX * width;
  const rightY = baseY - normalY * width;

  context.beginPath();
  context.moveTo(leftX, leftY);
  context.lineTo(tip.x, tip.y);
  context.lineTo(rightX, rightY);
  context.strokeStyle = stroke;
  context.lineWidth = 1.45;
  context.lineCap = "round";
  context.lineJoin = "round";
  context.stroke();
}

function buildBezierControlsWorld(start: WorldPoint, end: WorldPoint, backEdge: boolean): { c1: WorldPoint; c2: WorldPoint } {
  const deltaX = end.x - start.x;
  if (!backEdge && deltaX > 0) {
    const horizontalOffset = Math.max(36, Math.min(220, deltaX * 0.52));
    return {
      c1: {
        x: start.x + horizontalOffset,
        y: start.y,
      },
      c2: {
        x: end.x - horizontalOffset,
        y: end.y,
      },
    };
  }

  const horizontalOffset = Math.max(48, Math.min(240, Math.abs(deltaX) * 0.54 + 24));
  const direction = deltaX >= 0 ? 1 : -1;
  return {
    c1: {
      x: start.x + direction * horizontalOffset,
      y: start.y,
    },
    c2: {
      x: end.x - direction * horizontalOffset,
      y: end.y,
    },
  };
}

function normalizeVector(vector: WorldPoint): WorldPoint {
  const length = Math.hypot(vector.x, vector.y);
  if (length < 0.00001) {
    return { x: 1, y: 0 };
  }
  return {
    x: vector.x / length,
    y: vector.y / length,
  };
}

function clamp(value: number, min: number, max: number): number {
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return value;
}
