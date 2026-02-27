import { describe, expect, test } from "bun:test";
import {
  fitViewportToWorldRect,
  screenToWorld,
  worldToScreen,
  zoomViewportAtScreenPoint,
} from "./GraphViewport";

describe("GraphViewport", () => {
  test("round-trips world and screen coordinates", () => {
    const viewport = { tx: 120, ty: -42, scale: 1.35 };
    const world = { x: 88, y: 204 };

    const screen = worldToScreen(viewport, world);
    const roundTrip = screenToWorld(viewport, screen);

    expect(roundTrip.x).toBeCloseTo(world.x, 6);
    expect(roundTrip.y).toBeCloseTo(world.y, 6);
  });

  test("zooms around the cursor anchor", () => {
    const viewport = { tx: 40, ty: 20, scale: 1 };
    const cursor = { x: 300, y: 200 };

    const anchoredWorld = screenToWorld(viewport, cursor);
    const next = zoomViewportAtScreenPoint({
      viewport,
      nextScale: 1.8,
      screenX: cursor.x,
      screenY: cursor.y,
    });

    const anchoredAfterZoom = screenToWorld(next, cursor);

    expect(anchoredAfterZoom.x).toBeCloseTo(anchoredWorld.x, 6);
    expect(anchoredAfterZoom.y).toBeCloseTo(anchoredWorld.y, 6);
  });

  test("fits a world rect into a screen viewport", () => {
    const viewport = fitViewportToWorldRect({
      worldRect: {
        x: 0,
        y: 0,
        width: 1200,
        height: 800,
      },
      screenWidth: 800,
      screenHeight: 600,
      paddingPx: 40,
    });

    const topLeft = worldToScreen(viewport, { x: 0, y: 0 });
    const bottomRight = worldToScreen(viewport, { x: 1200, y: 800 });

    expect(topLeft.x).toBeGreaterThanOrEqual(0);
    expect(topLeft.y).toBeGreaterThanOrEqual(0);
    expect(bottomRight.x).toBeLessThanOrEqual(800);
    expect(bottomRight.y).toBeLessThanOrEqual(600);
  });
});
