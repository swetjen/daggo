import type { TimelineArtifact } from "./timelineArtifacts";

const RUN_MARKER_MIN_WIDTH_PX = 4;
const RUN_MARKER_SOFT_MAX_RATIO = 0.45;
const RUN_MARKER_SOFT_MAX_PX = 220;
const RUN_MARKER_TRULY_LONG_RATIO = 0.45;
const SCHEDULE_MARKER_WIDTH_PX = 9;
const MIN_VISIBLE_DURATION_MS = 500;

export type TimelineMarkerLayout = {
  leftPx: number;
  widthPx: number;
  zIndex: number;
};

export function computeTimelineMarkerLayout(input: {
  artifact: TimelineArtifact;
  windowStartMs: number;
  windowMs: number;
  trackWidthPx: number;
  nowMs: number;
}): TimelineMarkerLayout {
  const { artifact, windowStartMs, windowMs, trackWidthPx, nowMs } = input;

  const safeWindowMs = Math.max(1, windowMs);
  const safeTrackWidthPx = Math.max(1, trackWidthPx);
  const leftPx = ((artifact.timestampMs - windowStartMs) / safeWindowMs) * safeTrackWidthPx;

  if (artifact.state === "will_run" || artifact.kind === "schedule") {
    return {
      leftPx,
      widthPx: SCHEDULE_MARKER_WIDTH_PX,
      zIndex: 2,
    };
  }

  const durationMs = resolveRunDurationMs(artifact, nowMs);
  const proportionalWidth = (durationMs / safeWindowMs) * safeTrackWidthPx;
  const softMaxWidth = Math.max(16, Math.min(safeTrackWidthPx * RUN_MARKER_SOFT_MAX_RATIO, RUN_MARKER_SOFT_MAX_PX));
  const trulyLong = durationMs >= safeWindowMs * RUN_MARKER_TRULY_LONG_RATIO;
  const maxWidth = trulyLong ? safeTrackWidthPx : softMaxWidth;

  const widthPx =
    durationMs <= MIN_VISIBLE_DURATION_MS
      ? RUN_MARKER_MIN_WIDTH_PX
      : clamp(proportionalWidth, RUN_MARKER_MIN_WIDTH_PX, maxWidth);

  return {
    leftPx,
    widthPx,
    zIndex: artifact.state === "running" ? 4 : 3,
  };
}

function resolveRunDurationMs(artifact: TimelineArtifact, nowMs: number): number {
  if (artifact.durationMs > 0) {
    return artifact.durationMs;
  }

  if (artifact.endMs > artifact.startMs) {
    return artifact.endMs - artifact.startMs;
  }

  if (artifact.state === "running" && artifact.startMs > 0 && nowMs > artifact.startMs) {
    return nowMs - artifact.startMs;
  }

  return 0;
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
