import { listUpcomingScheduleTimestamps } from "./timelineSchedule";

const MINUTE_MS = 60 * 1000;
const DEFAULT_MAX_SCHEDULE_MARKERS = 720;

type TimelineArtifactState = "failed" | "succeeded" | "running" | "will_run";
type TimelineArtifactKind = "run" | "schedule";

export type TimelineArtifact = {
  id: string;
  kind: TimelineArtifactKind;
  state: TimelineArtifactState;
  timestampMs: number;
  startMs: number;
  endMs: number;
  durationMs: number;
  runKey: string;
  tooltip: string;
};

export type TimelineArtifactRun = {
  id: number;
  run_key: string;
  job_key: string;
  status: string;
  triggered_by: string;
  queued_at: string;
  started_at: string;
  completed_at: string;
};

export type TimelineArtifactSchedule = {
  schedule_key: string;
  cron_expr: string;
  timezone: string;
};

export function buildTimelineArtifacts(input: {
  jobKey: string;
  runs: TimelineArtifactRun[];
  schedules: TimelineArtifactSchedule[];
  nowMs: number;
  windowStartMs: number;
  windowEndMs: number;
  maxScheduleMarkers?: number;
}): TimelineArtifact[] {
  const artifacts: TimelineArtifact[] = [];
  const occupiedScheduleTicks = new Set<number>();

  for (const run of input.runs) {
    const timestampMs = timelineRunTimestampMs(run);
    const state = artifactStateFromRunStatus(run.status);
    const startMs = runStartMs(run);
    const endMs = parseTimestamp(run.completed_at);
    artifacts.push({
      id: `run-${run.id}`,
      kind: "run",
      state,
      timestampMs,
      startMs,
      endMs,
      durationMs: calculateDurationMs(startMs, endMs),
      runKey: run.run_key,
      tooltip: formatRunArtifactTooltip(run),
    });

    if (timestampMs > 0) {
      occupiedScheduleTicks.add(scheduleTickKey(timestampMs));
    }
  }

  const scheduleStartMs = Math.max(input.nowMs, input.windowStartMs);
  const maxScheduleMarkers =
    Number.isFinite(input.maxScheduleMarkers) && (input.maxScheduleMarkers ?? 0) > 0
      ? Math.floor(input.maxScheduleMarkers ?? DEFAULT_MAX_SCHEDULE_MARKERS)
      : DEFAULT_MAX_SCHEDULE_MARKERS;

  for (const schedule of input.schedules ?? []) {
    const upcomingTimes = listUpcomingScheduleTimestamps(
      {
        cronExpr: schedule.cron_expr,
        timezone: schedule.timezone,
      },
      scheduleStartMs,
      input.windowEndMs,
      { maxOccurrences: maxScheduleMarkers },
    );

    for (const timestampMs of upcomingTimes) {
      if (occupiedScheduleTicks.has(scheduleTickKey(timestampMs))) {
        continue;
      }
      artifacts.push({
        id: `schedule-${input.jobKey}-${schedule.schedule_key}-${timestampMs}`,
        kind: "schedule",
        state: "will_run",
        timestampMs,
        startMs: timestampMs,
        endMs: timestampMs,
        durationMs: 0,
        runKey: "",
        tooltip: formatScheduledArtifactTooltip(input.jobKey, timestampMs),
      });
    }
  }

  return artifacts.sort((left, right) => left.timestampMs - right.timestampMs || left.id.localeCompare(right.id));
}

function scheduleTickKey(timestampMs: number): number {
  return Math.floor(timestampMs / MINUTE_MS) * MINUTE_MS;
}

function timelineRunTimestampMs(run: TimelineArtifactRun): number {
  const timestampMs = runTimestampMs(run);
  if (timestampMs <= 0) {
    return 0;
  }
  if (isSchedulerTriggered(run.triggered_by)) {
    return scheduleTickKey(timestampMs);
  }
  return timestampMs;
}

function isSchedulerTriggered(triggeredBy: string): boolean {
  return (triggeredBy || "").trim().toLowerCase().startsWith("scheduler:");
}

function normalizeStatus(status: string): string {
  const normalized = (status || "").trim().toLowerCase();
  if (normalized === "cancelled") {
    return "canceled";
  }
  if (normalized === "in_progress") {
    return "running";
  }
  if (normalized === "idle") {
    return "pending";
  }
  return normalized;
}

function artifactStateFromRunStatus(status: string): TimelineArtifactState {
  const normalized = normalizeStatus(status);
  if (normalized === "failed" || normalized === "canceled") {
    return "failed";
  }
  if (normalized === "success" || normalized === "skipped") {
    return "succeeded";
  }
  return "running";
}

function runTimestampMs(run: TimelineArtifactRun): number {
  const candidates = [run.started_at, run.queued_at, run.completed_at];
  for (const candidate of candidates) {
    const ts = parseTimestamp(candidate);
    if (ts > 0) {
      return ts;
    }
  }
  return 0;
}

function runStartMs(run: TimelineArtifactRun): number {
  const startedMs = parseTimestamp(run.started_at);
  if (startedMs > 0) {
    return startedMs;
  }
  const queuedMs = parseTimestamp(run.queued_at);
  if (queuedMs > 0) {
    return queuedMs;
  }
  return runTimestampMs(run);
}

function calculateDurationMs(startMs: number, endMs: number): number {
  if (startMs <= 0 || endMs <= 0 || endMs < startMs) {
    return 0;
  }
  return endMs - startMs;
}

function parseTimestamp(value: string): number {
  if (!value) {
    return 0;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return 0;
  }
  return date.getTime();
}

function formatRunArtifactTooltip(run: TimelineArtifactRun): string {
  const startMs = runStartMs(run);
  const endMs = parseTimestamp(run.completed_at);
  const parts = [`job: ${run.job_key}`, `start: ${formatTooltipMoment(startMs)}`];
  if (endMs > 0) {
    parts.push(`end: ${formatTooltipMoment(endMs)}`);
  }
  return parts.join("\n");
}

function formatScheduledArtifactTooltip(jobKey: string, startMs: number): string {
  return [`job: ${jobKey}`, `start: ${formatTooltipMoment(startMs)}`].join("\n");
}

function formatTooltipMoment(ms: number): string {
  if (ms <= 0) {
    return "-";
  }
  return new Date(ms).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
