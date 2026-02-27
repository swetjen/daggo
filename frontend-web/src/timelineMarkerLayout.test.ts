import { describe, expect, test } from "bun:test";
import { computeTimelineMarkerLayout } from "./timelineMarkerLayout";
import type { TimelineArtifact } from "./timelineArtifacts";

function runArtifact(input: {
  id: string;
  state?: "failed" | "succeeded" | "running";
  timestampMs: number;
  startMs: number;
  endMs?: number;
  durationMs?: number;
}): TimelineArtifact {
  return {
    id: input.id,
    kind: "run",
    state: input.state ?? "succeeded",
    timestampMs: input.timestampMs,
    startMs: input.startMs,
    endMs: input.endMs ?? 0,
    durationMs: input.durationMs ?? 0,
    runKey: `run_${input.id}`,
    tooltip: "",
  };
}

function scheduleArtifact(timestampMs: number): TimelineArtifact {
  return {
    id: `schedule_${timestampMs}`,
    kind: "schedule",
    state: "will_run",
    timestampMs,
    startMs: timestampMs,
    endMs: timestampMs,
    durationMs: 0,
    runKey: "",
    tooltip: "",
  };
}

describe("computeTimelineMarkerLayout", () => {
  test("renders short runs at the minimum visible width", () => {
    const windowStartMs = Date.parse("2026-02-23T10:00:00.000Z");
    const shortRun = runArtifact({
      id: "short",
      timestampMs: windowStartMs,
      startMs: windowStartMs,
      endMs: windowStartMs + 2_000,
      durationMs: 2_000,
    });

    const layout = computeTimelineMarkerLayout({
      artifact: shortRun,
      windowStartMs,
      windowMs: 60 * 60 * 1000,
      trackWidthPx: 600,
      nowMs: windowStartMs,
    });

    expect(layout.widthPx).toBe(4);
  });

  test("renders 15-minute runs wider than 2-second runs", () => {
    const windowStartMs = Date.parse("2026-02-23T10:00:00.000Z");

    const shortLayout = computeTimelineMarkerLayout({
      artifact: runArtifact({
        id: "short",
        timestampMs: windowStartMs,
        startMs: windowStartMs,
        durationMs: 2_000,
      }),
      windowStartMs,
      windowMs: 60 * 60 * 1000,
      trackWidthPx: 600,
      nowMs: windowStartMs,
    });

    const longLayout = computeTimelineMarkerLayout({
      artifact: runArtifact({
        id: "long",
        timestampMs: windowStartMs,
        startMs: windowStartMs,
        durationMs: 15 * 60 * 1000,
      }),
      windowStartMs,
      windowMs: 60 * 60 * 1000,
      trackWidthPx: 600,
      nowMs: windowStartMs,
    });

    expect(longLayout.widthPx).toBeGreaterThan(shortLayout.widthPx);
  });

  test("expands running markers over time", () => {
    const startMs = Date.parse("2026-02-23T10:00:00.000Z");
    const running = runArtifact({
      id: "running",
      state: "running",
      timestampMs: startMs,
      startMs,
    });

    const early = computeTimelineMarkerLayout({
      artifact: running,
      windowStartMs: startMs,
      windowMs: 60 * 60 * 1000,
      trackWidthPx: 600,
      nowMs: startMs + 30_000,
    });

    const later = computeTimelineMarkerLayout({
      artifact: running,
      windowStartMs: startMs,
      windowMs: 60 * 60 * 1000,
      trackWidthPx: 600,
      nowMs: startMs + 10 * 60 * 1000,
    });

    expect(later.widthPx).toBeGreaterThan(early.widthPx);
    expect(later.zIndex).toBe(4);
  });

  test("allows overlap and stacks executed markers above scheduled placeholders", () => {
    const windowStartMs = Date.parse("2026-02-23T10:00:00.000Z");
    const run = runArtifact({
      id: "overlap",
      state: "succeeded",
      timestampMs: windowStartMs,
      startMs: windowStartMs,
      durationMs: 15 * 60 * 1000,
    });
    const schedule = scheduleArtifact(windowStartMs + 10 * 60 * 1000);

    const runLayout = computeTimelineMarkerLayout({
      artifact: run,
      windowStartMs,
      windowMs: 60 * 60 * 1000,
      trackWidthPx: 600,
      nowMs: windowStartMs,
    });

    const scheduleLayout = computeTimelineMarkerLayout({
      artifact: schedule,
      windowStartMs,
      windowMs: 60 * 60 * 1000,
      trackWidthPx: 600,
      nowMs: windowStartMs,
    });

    expect(runLayout.leftPx + runLayout.widthPx).toBeGreaterThan(scheduleLayout.leftPx);
    expect(runLayout.zIndex).toBeGreaterThan(scheduleLayout.zIndex);
  });
});
