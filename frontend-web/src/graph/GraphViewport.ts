import type { GraphViewport, WorldPoint, WorldRect } from "./types";

export const GRAPH_MIN_ZOOM = 0.28;
export const GRAPH_MAX_ZOOM = 2.5;

/**
 * Camera model:
 * screen = world * scale + (tx, ty)
 *
 * World coordinates are grid-aligned layout coordinates and are the
 * single source of truth for node and edge geometry.
 */

export function clampZoom(scale: number, minZoom = GRAPH_MIN_ZOOM, maxZoom = GRAPH_MAX_ZOOM): number {
  if (scale < minZoom) {
    return minZoom;
  }
  if (scale > maxZoom) {
    return maxZoom;
  }
  return scale;
}

export function worldToScreen(viewport: GraphViewport, point: WorldPoint): WorldPoint {
  return {
    x: point.x * viewport.scale + viewport.tx,
    y: point.y * viewport.scale + viewport.ty,
  };
}

export function screenToWorld(viewport: GraphViewport, point: WorldPoint): WorldPoint {
  return {
    x: (point.x - viewport.tx) / viewport.scale,
    y: (point.y - viewport.ty) / viewport.scale,
  };
}

export function worldRectToScreenRect(viewport: GraphViewport, rect: WorldRect): WorldRect {
  return {
    x: rect.x * viewport.scale + viewport.tx,
    y: rect.y * viewport.scale + viewport.ty,
    width: rect.width * viewport.scale,
    height: rect.height * viewport.scale,
  };
}

export function panViewport(viewport: GraphViewport, dx: number, dy: number): GraphViewport {
  return {
    ...viewport,
    tx: viewport.tx + dx,
    ty: viewport.ty + dy,
  };
}

export function zoomViewportAtScreenPoint(options: {
  viewport: GraphViewport;
  nextScale: number;
  screenX: number;
  screenY: number;
  minZoom?: number;
  maxZoom?: number;
}): GraphViewport {
  const { viewport, nextScale, screenX, screenY, minZoom = GRAPH_MIN_ZOOM, maxZoom = GRAPH_MAX_ZOOM } = options;
  const clampedScale = clampZoom(nextScale, minZoom, maxZoom);
  if (Math.abs(clampedScale - viewport.scale) < 0.00001) {
    return viewport;
  }

  const worldPoint = screenToWorld(viewport, { x: screenX, y: screenY });
  return {
    scale: clampedScale,
    tx: screenX - worldPoint.x * clampedScale,
    ty: screenY - worldPoint.y * clampedScale,
  };
}

export function fitViewportToWorldRect(options: {
  worldRect: WorldRect;
  screenWidth: number;
  screenHeight: number;
  paddingPx?: number;
  minZoom?: number;
  maxZoom?: number;
}): GraphViewport {
  const {
    worldRect,
    screenWidth,
    screenHeight,
    paddingPx = 40,
    minZoom = GRAPH_MIN_ZOOM,
    maxZoom = GRAPH_MAX_ZOOM,
  } = options;

  if (screenWidth <= 0 || screenHeight <= 0) {
    return {
      tx: 0,
      ty: 0,
      scale: 1,
    };
  }

  const availableWidth = Math.max(1, screenWidth - paddingPx * 2);
  const availableHeight = Math.max(1, screenHeight - paddingPx * 2);
  const widthScale = availableWidth / Math.max(1, worldRect.width);
  const heightScale = availableHeight / Math.max(1, worldRect.height);
  const scale = clampZoom(Math.min(widthScale, heightScale), minZoom, maxZoom);

  const scaledWidth = worldRect.width * scale;
  const scaledHeight = worldRect.height * scale;
  const tx = (screenWidth - scaledWidth) / 2 - worldRect.x * scale;
  const ty = (screenHeight - scaledHeight) / 2 - worldRect.y * scale;

  return {
    tx,
    ty,
    scale,
  };
}

export function viewportPanFromScreenDelta(viewport: GraphViewport, dx: number, dy: number): GraphViewport {
  return {
    ...viewport,
    tx: viewport.tx + dx,
    ty: viewport.ty + dy,
  };
}

export function zoomDeltaToScaleFactor(deltaY: number): number {
  const normalized = Math.max(-240, Math.min(240, deltaY));
  return Math.exp(-normalized * 0.0015);
}
