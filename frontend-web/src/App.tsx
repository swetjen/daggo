import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import "./index.css";
import { createClient } from "../api/client.gen.js";
import cronstrue from "cronstrue";
import { buildTimelineArtifacts, type TimelineArtifact } from "./timelineArtifacts";
import { computeTimelineMarkerLayout } from "./timelineMarkerLayout";
import { DagGraphCanvasHtml } from "./graph/DagGraphCanvasHtml";
import { DAG_NODE_HARD_CAP, buildDeterministicDagLayout } from "./graph/dagLayout";

const daggoLogoSrc = "/assets/daggo.png";

type JobNode = {
  step_key: string;
  display_name: string;
  description: string;
  kind: string;
  depends_on: string[];
};

type JobEdge = {
  from_step_key: string;
  to_step_key: string;
};

type JobSchedule = {
  schedule_key: string;
  cron_expr: string;
  timezone: string;
  is_enabled: boolean;
  description: string;
};

type Job = {
  id: number;
  job_key: string;
  display_name: string;
  description: string;
  default_params_json: string;
  scheduling_paused: boolean;
  nodes: JobNode[];
  edges: JobEdge[];
  schedules: JobSchedule[];
};

type RunSummary = {
  id: number;
  run_key: string;
  job_id: number;
  job_key: string;
  status: string;
  triggered_by: string;
  queued_at: string;
  started_at: string;
  completed_at: string;
  parent_run_id: number;
  rerun_step_key: string;
  error_message: string;
  pending_steps: number;
  running_steps: number;
  success_steps: number;
  failed_steps: number;
  skipped_steps: number;
};

type RunStep = {
  step_key: string;
  status: string;
  attempt: number;
  started_at: string;
  completed_at: string;
  duration_ms: number;
  output_json: string;
  error_message: string;
  log_excerpt: string;
};

type RunEvent = {
  id: number;
  step_key: string;
  event_type: string;
  level: string;
  message: string;
  event_data_json: string;
  created_at: string;
};

type JobListResponse = {
  data?: Job[];
  total?: number;
  error?: string;
};

type RunsListResponse = {
  data?: RunSummary[];
  total?: number;
  error?: string;
};

type RunCreateResponse = {
  run?: RunSummary;
  error?: string;
};

type JobSchedulingUpdateResponse = {
  job?: Job;
  error?: string;
};

type RunTerminateResponse = {
  run?: RunSummary;
  error?: string;
};

type RunByIDResponse = {
  run?: {
    summary: RunSummary;
    steps: RunStep[];
  };
  error?: string;
};

type RunEventsGetManyResponse = {
  data?: RunEvent[];
  total?: number;
  error?: string;
};

type SchedulesResponse = {
  data?: {
    id: number;
    job_id: number;
    job_key: string;
    schedule_key: string;
    cron_expr: string;
    timezone: string;
    is_enabled: boolean;
    description: string;
  }[];
  total?: number;
  error?: string;
};

type ScheduleRow = NonNullable<SchedulesResponse["data"]>[number];

type NavSection = "overview" | "jobs" | "runs";

type AppRoute = {
  section: NavSection;
  path: string;
  jobKey: string;
  runKey: string;
};

type RunHealthPopoverState = {
  run: RunSummary | null;
  jobLabel: string;
  left: number;
  top: number;
  placement: "below" | "above";
};

const STATUS_ORDER = ["failed", "canceled", "running", "queued", "pending", "success", "skipped"];
const RUNS_POLL_INTERVAL_MS = 15_000;
const RUN_DETAIL_POLL_INTERVAL_MS = 5_000;
const RUN_EVENTS_PAGE_SIZE = 200;
const NAV_ITEMS: { key: NavSection; label: string; detail: string }[] = [
  { key: "overview", label: "Overview", detail: "Temporal operations dashboard" },
  { key: "jobs", label: "Jobs", detail: "Definitions, schedules, structure" },
  { key: "runs", label: "Runs", detail: "Historical and active executions" },
];
const OVERVIEW_WINDOWS = [1, 6, 12, 24] as const;
type OverviewWindow = (typeof OVERVIEW_WINDOWS)[number];
const OVERVIEW_TRACK_MIN_WIDTH = 780;
const OVERVIEW_TRACK_PX_PER_HOUR: Record<OverviewWindow, number> = {
  1: 220,
  6: 130,
  12: 72,
  24: 48,
};
const MINUTE_MS = 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * HOUR_MS;
const OVERVIEW_PAST_RATIO = 0.75;
const OVERVIEW_TOOLTIP_DELAY_MS = 200;
const MAX_SCHEDULE_MARKERS_PER_SCHEDULE = 720;
const TIMELINE_LIVE_REBASE_MS = 60 * 1000;
const RUNS_WINDOWS = [0, 1, 6, 24] as const;
type RunsWindow = (typeof RUNS_WINDOWS)[number];
type RunsQuickFilter = "all" | "backfills" | "queued" | "in_progress" | "failed" | "scheduled";
type RunsSort = "newest" | "oldest" | "duration_desc";
type RunStepGroupKey = "preparing" | "executing" | "failed" | "succeeded" | "not_executed";
type EventStreamFilter = "all" | "stdout" | "stderr";
type JobDetailTab = "overview" | "runs";
type ThemeMode = "dark" | "light";

const THEME_STORAGE_KEY = "daggo.theme";

function resolveInitialThemeMode(): ThemeMode {
  if (typeof window === "undefined") {
    return "dark";
  }
  try {
    const persisted = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (persisted === "dark" || persisted === "light") {
      return persisted;
    }
  } catch {
    // Ignore storage errors in restricted environments.
  }
  if (typeof window.matchMedia === "function") {
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  return "dark";
}

const INITIAL_THEME_MODE = resolveInitialThemeMode();

if (typeof document !== "undefined") {
  document.documentElement.setAttribute("data-theme", INITIAL_THEME_MODE);
  document.documentElement.style.colorScheme = INITIAL_THEME_MODE;
}

const RUNS_QUICK_FILTER_ITEMS: { key: RunsQuickFilter; label: string }[] = [
  { key: "all", label: "All" },
  { key: "backfills", label: "Backfills" },
  { key: "queued", label: "Queued" },
  { key: "in_progress", label: "In Progress" },
  { key: "failed", label: "Failed" },
  { key: "scheduled", label: "Scheduled" },
];

const RUNS_SORT_ITEMS: { key: RunsSort; label: string }[] = [
  { key: "newest", label: "Newest first" },
  { key: "oldest", label: "Oldest first" },
  { key: "duration_desc", label: "Longest duration" },
];

const RUN_STEP_GROUPS: { key: RunStepGroupKey; label: string }[] = [
  { key: "preparing", label: "Preparing" },
  { key: "executing", label: "Executing" },
  { key: "failed", label: "Failed" },
  { key: "succeeded", label: "Succeeded" },
  { key: "not_executed", label: "Not Executed" },
];

const RUNS_PAGE_SIZE = 50;
const JOB_HEALTH_RUN_COUNT = 5;
const RUN_HEALTH_POPOVER_DELAY_MS = 120;

function normalizePathname(pathname: string): string {
  const withSlash = pathname.startsWith("/") ? pathname : `/${pathname}`;
  if (withSlash.length > 1) {
    return withSlash.replace(/\/+$/, "");
  }
  return withSlash;
}

function parseRoute(pathname: string): AppRoute {
  const normalized = normalizePathname(pathname);
  if (normalized === "/" || normalized === "/overview") {
    return {
      section: "overview",
      path: "/",
      jobKey: "",
      runKey: "",
    };
  }

  const jobMatch = normalized.match(/^\/jobs\/([^/]+)$/);
  if (jobMatch) {
    const decodedJobKey = decodePathSegment(jobMatch[1]);
    if (decodedJobKey) {
      return {
        section: "jobs",
        path: `/jobs/${encodeURIComponent(decodedJobKey)}`,
        jobKey: decodedJobKey,
        runKey: "",
      };
    }
  }
  if (normalized === "/jobs") {
    return {
      section: "jobs",
      path: "/jobs",
      jobKey: "",
      runKey: "",
    };
  }

  const runMatch = normalized.match(/^\/runs\/([^/]+)$/);
  if (runMatch) {
    const decodedRunKey = decodePathSegment(runMatch[1]);
    if (decodedRunKey) {
      return {
        section: "runs",
        path: `/runs/${encodeURIComponent(decodedRunKey)}`,
        jobKey: "",
        runKey: decodedRunKey,
      };
    }
  }
  if (normalized === "/runs") {
    return {
      section: "runs",
      path: "/runs",
      jobKey: "",
      runKey: "",
    };
  }

  return {
    section: "overview",
    path: "/",
    jobKey: "",
    runKey: "",
  };
}

function decodePathSegment(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return "";
  }
}

export function App() {
  const api = useMemo(() => createClient(window.location.origin), []);
  const initialRoute = useMemo(() => parseRoute(window.location.pathname), []);
  const [themeMode, setThemeMode] = useState<ThemeMode>(INITIAL_THEME_MODE);

  const [activeSection, setActiveSection] = useState<NavSection>(initialRoute.section);
  const [jobsPage, setJobsPage] = useState<"list" | "detail">(
    initialRoute.section === "jobs" && initialRoute.jobKey.length > 0 ? "detail" : "list",
  );
  const [runsPage, setRunsPage] = useState<"list" | "detail">(
    initialRoute.section === "runs" && initialRoute.runKey.length > 0 ? "detail" : "list",
  );
  const [jobs, setJobs] = useState<Job[]>([]);
  const [allRuns, setAllRuns] = useState<RunSummary[]>([]);
  const [scheduleRows, setScheduleRows] = useState<SchedulesResponse["data"]>([]);
  const [routeJobKey, setRouteJobKey] = useState<string>(initialRoute.section === "jobs" ? initialRoute.jobKey : "");
  const [routeRunKey, setRouteRunKey] = useState<string>(initialRoute.section === "runs" ? initialRoute.runKey : "");

  const [selectedJobKey, setSelectedJobKey] = useState<string>("");
  const [selectedRunID, setSelectedRunID] = useState<number>(0);
  const [runDetail, setRunDetail] = useState<{ summary: RunSummary; steps: RunStep[] } | null>(null);
  const [runEvents, setRunEvents] = useState<RunEvent[]>([]);
  const [runEventsTotal, setRunEventsTotal] = useState(0);
  const [runEventsBusy, setRunEventsBusy] = useState(false);
  const [selectedNodeKey, setSelectedNodeKey] = useState<string>("");
  const [runLaunchPendingByJobKey, setRunLaunchPendingByJobKey] = useState<Record<string, boolean>>({});
  const [scheduleUpdatePendingByKey, setScheduleUpdatePendingByKey] = useState<Record<string, boolean>>({});
  const [runHealthPopover, setRunHealthPopover] = useState<RunHealthPopoverState | null>(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [overviewQuery, setOverviewQuery] = useState("");
  const [overviewWindowHours, setOverviewWindowHours] = useState<OverviewWindow>(6);
  const [overviewAnchorMs, setOverviewAnchorMs] = useState(() => Date.now());
  const [overviewFollowNow, setOverviewFollowNow] = useState(true);
  const timelineScrollRef = useRef<HTMLDivElement | null>(null);
  const timelineTooltipTimerRef = useRef<number | null>(null);
  const [timelineViewportWidth, setTimelineViewportWidth] = useState(0);
  const [timelineTooltip, setTimelineTooltip] = useState<{ text: string; x: number; y: number } | null>(null);
  const runEventsViewportRef = useRef<HTMLDivElement | null>(null);
  const runEventsScrollToBottomPendingRef = useRef(false);
  const [refreshClockMs, setRefreshClockMs] = useState(() => Date.now());
  const [nextAutoRefreshAtMs, setNextAutoRefreshAtMs] = useState(() => Date.now() + RUNS_POLL_INTERVAL_MS);
  const [lastRefreshAtMs, setLastRefreshAtMs] = useState(0);
  const [refreshControlBusy, setRefreshControlBusy] = useState(false);
  const autoRefreshInFlightRef = useRef(false);

  const [runsJobFilter, setRunsJobFilter] = useState<string>("all");
  const [runsStatusFilter, setRunsStatusFilter] = useState<string>("all");
  const [runsWindowHours, setRunsWindowHours] = useState<RunsWindow>(0);
  const [runsSearch, setRunsSearch] = useState<string>("");
  const [runsQuickFilter, setRunsQuickFilter] = useState<RunsQuickFilter>("all");
  const [runsSort, setRunsSort] = useState<RunsSort>("newest");
  const [runsPageIndex, setRunsPageIndex] = useState(0);
  const [jobsQuery, setJobsQuery] = useState("");
  const runHealthPopoverRef = useRef<HTMLButtonElement | null>(null);
  const runHealthPopoverShowTimerRef = useRef<number | null>(null);
  const runHealthPopoverHideTimerRef = useRef<number | null>(null);

  const [runHideUnexecuted, setRunHideUnexecuted] = useState(false);
  const [runSelectedStepKey, setRunSelectedStepKey] = useState("");
  const [runHoveredStepKey, setRunHoveredStepKey] = useState("");
  const [runStepGroupFilter, setRunStepGroupFilter] = useState<RunStepGroupKey | "all">("all");
  const [collapsedRunGroups, setCollapsedRunGroups] = useState<RunStepGroupKey[]>([]);
  const [jobDetailTab, setJobDetailTab] = useState<JobDetailTab>("overview");

  const [runEventLevelFilter, setRunEventLevelFilter] = useState("all");
  const [runEventStreamFilter, setRunEventStreamFilter] = useState<EventStreamFilter>("all");
  const [runEventSearch, setRunEventSearch] = useState("");
  const [runEventStepFilter, setRunEventStepFilter] = useState("all");
  const [collapsedRunEventIDs, setCollapsedRunEventIDs] = useState<number[]>([]);

  const sortedRuns = useMemo(() => sortRunsByFreshness(allRuns), [allRuns]);
  const selectedJob = jobs.find((job) => job.job_key === selectedJobKey) ?? null;
  const scheduleRowsByJob = useMemo(() => {
    const byJob: Record<string, ScheduleRow[]> = {};
    for (const schedule of scheduleRows ?? []) {
      const rows = byJob[schedule.job_key] ?? [];
      rows.push(schedule);
      byJob[schedule.job_key] = rows;
    }
    return byJob;
  }, [scheduleRows]);
  const enabledSchedulesByJob = useMemo(() => {
    const byJob: Record<string, ScheduleRow[]> = {};
    for (const schedule of scheduleRows ?? []) {
      if (!schedule.is_enabled) {
        continue;
      }
      const rows = byJob[schedule.job_key] ?? [];
      rows.push(schedule);
      byJob[schedule.job_key] = rows;
    }
    return byJob;
  }, [scheduleRows]);

  const stepStatusByKey = useMemo(() => {
    const statuses: Record<string, string> = {};
    for (const step of runDetail?.steps ?? []) {
      statuses[step.step_key] = step.status;
    }
    return statuses;
  }, [runDetail]);
  const runStepByKey = useMemo(() => {
    const steps: Record<string, RunStep> = {};
    for (const step of runDetail?.steps ?? []) {
      steps[step.step_key] = step;
    }
    return steps;
  }, [runDetail]);

  const latestRunByJob = useMemo(() => {
    const byJob: Record<string, RunSummary> = {};
    for (const run of sortedRuns) {
      if (!byJob[run.job_key]) {
        byJob[run.job_key] = run;
      }
    }
    return byJob;
  }, [sortedRuns]);
  const recentRunsByJob = useMemo(() => {
    const byJob: Record<string, RunSummary[]> = {};
    for (const run of sortedRuns) {
      const rows = byJob[run.job_key] ?? [];
      if (rows.length >= JOB_HEALTH_RUN_COUNT) {
        continue;
      }
      rows.push(run);
      byJob[run.job_key] = rows;
    }
    return byJob;
  }, [sortedRuns]);
  const jobsQueryText = jobsQuery.trim().toLowerCase();
  const jobsForList = useMemo(() => {
    const sorted = [...jobs].sort((left, right) => {
      const leftLabel = left.display_name || left.job_key;
      const rightLabel = right.display_name || right.job_key;
      return leftLabel.localeCompare(rightLabel);
    });
    if (!jobsQueryText) {
      return sorted;
    }
    return sorted.filter((job) => {
      const haystack = `${job.display_name} ${job.job_key} ${job.description}`.toLowerCase();
      return haystack.includes(jobsQueryText);
    });
  }, [jobs, jobsQueryText]);
  const runLaunchBusy = Boolean(selectedJobKey && runLaunchPendingByJobKey[selectedJobKey]);

  const dagLayout = useMemo(
    () =>
      buildDeterministicDagLayout(selectedJob, stepStatusByKey, {
        normalizeStatus,
      }),
    [selectedJob, stepStatusByKey],
  );
  const jobVisibleStepKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const node of dagLayout.nodes) {
      keys.add(node.step_key);
    }
    return keys;
  }, [dagLayout.nodes]);
  const jobVisibleNodes = useMemo(
    () => dagLayout.nodes.filter((node) => jobVisibleStepKeys.has(node.step_key)),
    [dagLayout.nodes, jobVisibleStepKeys],
  );
  const jobVisibleEdges = useMemo(
    () => dagLayout.edges.filter((edge) => jobVisibleStepKeys.has(edge.from) && jobVisibleStepKeys.has(edge.to)),
    [dagLayout.edges, jobVisibleStepKeys],
  );
  const selectedNode = useMemo(
    () => dagLayout.nodes.find((node) => node.step_key === selectedNodeKey) ?? null,
    [dagLayout.nodes, selectedNodeKey],
  );
  const selectedNodeDependencies = useMemo(
    () => (selectedNodeKey ? dagLayout.dependenciesByKey[selectedNodeKey] ?? [] : []),
    [dagLayout.dependenciesByKey, selectedNodeKey],
  );
  const selectedNodeDependents = useMemo(
    () => (selectedNodeKey ? dagLayout.dependentsByKey[selectedNodeKey] ?? [] : []),
    [dagLayout.dependentsByKey, selectedNodeKey],
  );
  const relatedNodeSet = useMemo(() => {
    if (!selectedNodeKey) return new Set<string>();
    return new Set<string>([selectedNodeKey, ...selectedNodeDependencies, ...selectedNodeDependents]);
  }, [selectedNodeDependencies, selectedNodeDependents, selectedNodeKey]);

  const selectedJobRuns = useMemo(
    () => sortedRuns.filter((run) => run.job_key === selectedJobKey).slice(0, 20),
    [selectedJobKey, sortedRuns],
  );
  const selectedJobRunCount = useMemo(
    () => sortedRuns.filter((run) => run.job_key === selectedJobKey).length,
    [selectedJobKey, sortedRuns],
  );

  const runDetailJob = useMemo(() => {
    if (!runDetail?.summary.job_key) {
      return null;
    }
    return jobs.find((job) => job.job_key === runDetail.summary.job_key) ?? null;
  }, [jobs, runDetail]);
  const runIsActive = useMemo(() => {
    if (!runDetail) {
      return false;
    }
    const status = normalizeStatus(runDetail.summary.status);
    return status === "running" || status === "queued";
  }, [runDetail]);

  const runDagLayout = useMemo(
    () =>
      buildDeterministicDagLayout(runDetailJob, stepStatusByKey, {
        normalizeStatus,
      }),
    [runDetailJob, stepStatusByKey],
  );
  const runVisibleStepKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const node of runDagLayout.nodes) {
      const normalizedStatus = normalizeStatus(stepStatusByKey[node.step_key] ?? "pending");
      if (runHideUnexecuted && runStepGroupFromStatus(normalizedStatus) === "not_executed") {
        continue;
      }
      if (runStepGroupFilter !== "all" && runStepGroupFromStatus(normalizedStatus) !== runStepGroupFilter) {
        continue;
      }
      keys.add(node.step_key);
    }
    return keys;
  }, [runDagLayout.nodes, runHideUnexecuted, runStepGroupFilter, stepStatusByKey]);

  const runVisibleNodes = useMemo(
    () => runDagLayout.nodes.filter((node) => runVisibleStepKeys.has(node.step_key)),
    [runDagLayout.nodes, runVisibleStepKeys],
  );
  const runSelectedStepVisible = runSelectedStepKey.length > 0 && runVisibleStepKeys.has(runSelectedStepKey);
  const runVisibleEdges = useMemo(
    () => runDagLayout.edges.filter((edge) => runVisibleStepKeys.has(edge.from) && runVisibleStepKeys.has(edge.to)),
    [runDagLayout.edges, runVisibleStepKeys],
  );
  const runSelectedStepDependencies = useMemo(
    () => (runSelectedStepKey ? runDagLayout.dependenciesByKey[runSelectedStepKey] ?? [] : []),
    [runDagLayout.dependenciesByKey, runSelectedStepKey],
  );
  const runSelectedStepDependents = useMemo(
    () => (runSelectedStepKey ? runDagLayout.dependentsByKey[runSelectedStepKey] ?? [] : []),
    [runDagLayout.dependentsByKey, runSelectedStepKey],
  );
  const runRelatedNodeSet = useMemo(() => {
    if (!runSelectedStepKey) {
      return new Set<string>();
    }
    return new Set<string>([runSelectedStepKey, ...runSelectedStepDependencies, ...runSelectedStepDependents]);
  }, [runSelectedStepDependencies, runSelectedStepDependents, runSelectedStepKey]);

  const runStepCountsByGroup = useMemo(() => {
    const counts: Record<RunStepGroupKey, number> = {
      preparing: 0,
      executing: 0,
      failed: 0,
      succeeded: 0,
      not_executed: 0,
    };
    const keys = runDagLayout.nodes.map((node) => node.step_key);
    for (const stepKey of keys) {
      const status = normalizeStatus(stepStatusByKey[stepKey] ?? "pending");
      counts[runStepGroupFromStatus(status)] += 1;
    }
    return counts;
  }, [runDagLayout.nodes, stepStatusByKey]);

  const runStepKeysByGroup = useMemo(() => {
    const groups: Record<RunStepGroupKey, string[]> = {
      preparing: [],
      executing: [],
      failed: [],
      succeeded: [],
      not_executed: [],
    };
    for (const node of runDagLayout.nodes) {
      const status = normalizeStatus(stepStatusByKey[node.step_key] ?? "pending");
      groups[runStepGroupFromStatus(status)].push(node.step_key);
    }
    for (const key of Object.keys(groups) as RunStepGroupKey[]) {
      groups[key].sort((left, right) => left.localeCompare(right));
    }
    return groups;
  }, [runDagLayout.nodes, stepStatusByKey]);

  const runEventLevels = useMemo(() => {
    const levels = new Set<string>();
    for (const event of runEvents) {
      const level = normalizeEventLevel(event.level);
      if (level) {
        levels.add(level);
      }
    }
    return Array.from(levels).sort();
  }, [runEvents]);

  const runEventSearchText = runEventSearch.trim().toLowerCase();
  const filteredRunEvents = useMemo(() => {
    const allowedSteps =
      runStepGroupFilter === "all" ? null : new Set<string>((runStepKeysByGroup[runStepGroupFilter] ?? []).map((key) => key));
    return runEvents.filter((event) => {
      if (allowedSteps && event.step_key && !allowedSteps.has(event.step_key)) {
        return false;
      }
      if (runEventStepFilter !== "all" && event.step_key !== runEventStepFilter) {
        return false;
      }
      if (runEventLevelFilter !== "all" && normalizeEventLevel(event.level) !== runEventLevelFilter) {
        return false;
      }
      if (runEventStreamFilter !== "all" && !eventMatchesStream(event, runEventStreamFilter)) {
        return false;
      }
      if (runEventSearchText) {
        const haystack = `${event.step_key} ${event.event_type} ${event.level} ${event.message}`.toLowerCase();
        if (!haystack.includes(runEventSearchText)) {
          return false;
        }
      }
      return true;
    });
  }, [
    runEventLevelFilter,
    runEventSearchText,
    runEventStepFilter,
    runEventStreamFilter,
    runEvents,
    runStepGroupFilter,
    runStepKeysByGroup,
  ]);

  const overviewFilter = overviewQuery.trim().toLowerCase();
  const overviewJobs = useMemo(() => {
    const ordered = [...jobs].sort((left, right) => {
      const leftLabel = left.display_name || left.job_key;
      const rightLabel = right.display_name || right.job_key;
      return leftLabel.localeCompare(rightLabel);
    });
    if (!overviewFilter) {
      return ordered;
    }
    return ordered.filter((job) => {
      const displayName = (job.display_name || "").toLowerCase();
      const jobKey = (job.job_key || "").toLowerCase();
      return displayName.includes(overviewFilter) || jobKey.includes(overviewFilter);
    });
  }, [jobs, overviewFilter]);

  const overviewWindowMs = overviewWindowHours * 60 * 60 * 1000;
  const overviewPastMs = Math.round(overviewWindowMs * OVERVIEW_PAST_RATIO);
  const overviewFutureMs = overviewWindowMs - overviewPastMs;
  const overviewStartMs = overviewAnchorMs - overviewPastMs;
  const overviewEndMs = overviewAnchorMs + overviewFutureMs;
  const overviewRangeMs = overviewEndMs - overviewStartMs;
  const timelineLabelWidth = timelineViewportWidth > 0 && timelineViewportWidth <= 900 ? 200 : 260;
  const scaledTimelineTrackWidth = Math.max(
    OVERVIEW_TRACK_MIN_WIDTH,
    Math.round(overviewWindowHours * OVERVIEW_TRACK_PX_PER_HOUR[overviewWindowHours]),
  );
  const timelineTrackWidth = Math.max(scaledTimelineTrackWidth, Math.max(0, timelineViewportWidth - timelineLabelWidth));
  const overviewLiveShiftMs = overviewFollowNow ? Math.max(0, refreshClockMs - overviewAnchorMs) : 0;
  const overviewLiveShiftPx = Math.min(timelineTrackWidth, (overviewLiveShiftMs / overviewRangeMs) * timelineTrackWidth);
  const overviewDisplayStartMs = overviewStartMs + overviewLiveShiftMs;
  const overviewDisplayEndMs = overviewEndMs + overviewLiveShiftMs;
  const overviewRenderEndMs = overviewEndMs + TIMELINE_LIVE_REBASE_MS;
  const overviewNowLeftPct = Math.min(100, Math.max(0, ((overviewAnchorMs - overviewStartMs) / overviewRangeMs) * 100));
  const anchoredToNow = overviewFollowNow;
  const refreshCountdownMs = Math.max(0, nextAutoRefreshAtMs - refreshClockMs);
  const refreshCountdownLabel = formatRefreshCountdown(refreshCountdownMs);
  const refreshJustUpdated = lastRefreshAtMs > 0 && refreshClockMs - lastRefreshAtMs < 1600;
  const overviewScheduleNowMs = Math.floor(refreshClockMs / MINUTE_MS) * MINUTE_MS;
  const timelineShiftStyle = useMemo(
    () => ({
      transform: `translateX(-${overviewLiveShiftPx}px)`,
      transition: overviewFollowNow ? "transform 1000ms linear" : "none",
    }),
    [overviewFollowNow, overviewLiveShiftPx],
  );

  const overviewRunsInWindow = useMemo(() => {
    return sortedRuns.filter((run) => {
      const ts = runTimestampMs(run);
      return ts >= overviewStartMs && ts <= overviewRenderEndMs;
    });
  }, [overviewRenderEndMs, overviewStartMs, sortedRuns]);

  const overviewRunsByJob = useMemo(() => {
    const byJob = new Map<string, RunSummary[]>();
    for (const run of overviewRunsInWindow) {
      const rows = byJob.get(run.job_key) ?? [];
      rows.push(run);
      byJob.set(run.job_key, rows);
    }
    for (const rows of byJob.values()) {
      rows.sort((left, right) => runTimestampMs(left) - runTimestampMs(right) || left.id - right.id);
    }
    return byJob;
  }, [overviewRunsInWindow]);

  const overviewArtifactsByJob = useMemo(() => {
    const byJob = new Map<string, TimelineArtifact[]>();
    for (const job of overviewJobs) {
      const runsForJob = overviewRunsByJob.get(job.job_key) ?? [];
      const artifacts = buildTimelineArtifacts({
        jobKey: job.job_key,
        runs: runsForJob,
        schedules: enabledSchedulesByJob[job.job_key] ?? [],
        nowMs: overviewScheduleNowMs,
        windowStartMs: overviewStartMs,
        windowEndMs: overviewRenderEndMs,
        maxScheduleMarkers: MAX_SCHEDULE_MARKERS_PER_SCHEDULE,
      });
      byJob.set(job.job_key, artifacts);
    }
    return byJob;
  }, [enabledSchedulesByJob, overviewJobs, overviewRenderEndMs, overviewRunsByJob, overviewScheduleNowMs, overviewStartMs]);

  const overviewDateMarkers = useMemo(
    () => buildTimelineDateMarkers(overviewStartMs, overviewEndMs, overviewRangeMs),
    [overviewEndMs, overviewRangeMs, overviewStartMs],
  );

  const overviewTicks = useMemo(() => {
    const firstTickMs = Math.floor(overviewStartMs / HOUR_MS) * HOUR_MS;
    const lastTickMs = Math.ceil(overviewEndMs / HOUR_MS) * HOUR_MS;
    const ticks: { key: number; leftPct: number; timeLabel: string }[] = [];
    for (let ts = firstTickMs; ts <= lastTickMs; ts += HOUR_MS) {
      const rawPct = ((ts - overviewStartMs) / overviewRangeMs) * 100;
      if (rawPct < -0.5 || rawPct > 100.5) {
        continue;
      }
      const leftPct = Math.min(100, Math.max(0, rawPct));
      const hasNearDuplicate = ticks.some((tick) => Math.abs(tick.leftPct - leftPct) < 0.6);
      if (hasNearDuplicate) {
        continue;
      }
      ticks.push({
        key: ts,
        leftPct,
        timeLabel: formatTimelineHour(ts),
      });
    }
    if (ticks.length > 0) {
      return ticks;
    }
    const snapped = Math.round(overviewEndMs / HOUR_MS) * HOUR_MS;
    const bounded = Math.min(Math.max(snapped, overviewStartMs), overviewEndMs);
    return [
      {
        key: snapped,
        leftPct: ((bounded - overviewStartMs) / overviewRangeMs) * 100,
        timeLabel: formatTimelineHour(snapped),
      },
    ];
  }, [overviewEndMs, overviewRangeMs, overviewStartMs]);

  const overviewStats = useMemo(() => {
    const runningNow = sortedRuns.filter((run) => {
      const status = normalizeStatus(run.status);
      return status === "running" || status === "queued";
    }).length;
    const failedInWindow = overviewRunsInWindow.filter((run) => normalizeStatus(run.status) === "failed").length;
    const successInWindow = overviewRunsInWindow.filter((run) => normalizeStatus(run.status) === "success").length;
    const enabledSchedules = (scheduleRows ?? []).filter((schedule) => schedule.is_enabled).length;
    const quietJobs = overviewJobs.filter((job) => (overviewRunsByJob.get(job.job_key) ?? []).length === 0).length;
    return {
      runningNow,
      failedInWindow,
      successInWindow,
      enabledSchedules,
      quietJobs,
      totalRunsInWindow: overviewRunsInWindow.length,
    };
  }, [overviewJobs, overviewRunsByJob, overviewRunsInWindow, scheduleRows, sortedRuns]);

  const hideTimelineTooltip = useCallback(() => {
    if (timelineTooltipTimerRef.current !== null) {
      window.clearTimeout(timelineTooltipTimerRef.current);
      timelineTooltipTimerRef.current = null;
    }
    setTimelineTooltip(null);
  }, []);

  const queueTimelineTooltip = useCallback((target: HTMLButtonElement, text: string) => {
    if (!text) {
      return;
    }
    if (timelineTooltipTimerRef.current !== null) {
      window.clearTimeout(timelineTooltipTimerRef.current);
      timelineTooltipTimerRef.current = null;
    }
    const viewport = timelineScrollRef.current;
    if (!viewport) {
      return;
    }
    const markerRect = target.getBoundingClientRect();
    const viewportRect = viewport.getBoundingClientRect();
    const x = markerRect.left - viewportRect.left + viewport.scrollLeft + markerRect.width / 2;
    const y = markerRect.top - viewportRect.top + viewport.scrollTop - 8;
    timelineTooltipTimerRef.current = window.setTimeout(() => {
      setTimelineTooltip({ text, x, y });
      timelineTooltipTimerRef.current = null;
    }, OVERVIEW_TOOLTIP_DELAY_MS);
  }, []);

  const clearRunHealthPopoverTimers = useCallback(() => {
    if (runHealthPopoverShowTimerRef.current !== null) {
      window.clearTimeout(runHealthPopoverShowTimerRef.current);
      runHealthPopoverShowTimerRef.current = null;
    }
    if (runHealthPopoverHideTimerRef.current !== null) {
      window.clearTimeout(runHealthPopoverHideTimerRef.current);
      runHealthPopoverHideTimerRef.current = null;
    }
  }, []);

  const queueRunHealthPopover = useCallback(
    (target: HTMLElement, run: RunSummary | null, jobLabel: string) => {
      if (runHealthPopoverHideTimerRef.current !== null) {
        window.clearTimeout(runHealthPopoverHideTimerRef.current);
        runHealthPopoverHideTimerRef.current = null;
      }
      if (runHealthPopoverShowTimerRef.current !== null) {
        window.clearTimeout(runHealthPopoverShowTimerRef.current);
        runHealthPopoverShowTimerRef.current = null;
      }
      runHealthPopoverShowTimerRef.current = window.setTimeout(() => {
        const rect = target.getBoundingClientRect();
        const viewportPadding = 12;
        const popoverWidth = 244;
        const popoverHeight = 140;
        const anchorGap = 8;
        let left = rect.left + rect.width / 2 - popoverWidth / 2;
        left = Math.max(viewportPadding, Math.min(left, window.innerWidth - popoverWidth - viewportPadding));
        let top = rect.bottom + anchorGap;
        let placement: "below" | "above" = "below";
        if (top + popoverHeight > window.innerHeight - viewportPadding) {
          placement = "above";
          top = rect.top - anchorGap;
        }
        setRunHealthPopover({
          run,
          jobLabel,
          left,
          top,
          placement,
        });
        runHealthPopoverShowTimerRef.current = null;
      }, RUN_HEALTH_POPOVER_DELAY_MS);
    },
    [],
  );

  const scheduleRunHealthPopoverHide = useCallback((delayMs = 90) => {
    if (runHealthPopoverShowTimerRef.current !== null) {
      window.clearTimeout(runHealthPopoverShowTimerRef.current);
      runHealthPopoverShowTimerRef.current = null;
    }
    if (runHealthPopoverHideTimerRef.current !== null) {
      window.clearTimeout(runHealthPopoverHideTimerRef.current);
      runHealthPopoverHideTimerRef.current = null;
    }
    runHealthPopoverHideTimerRef.current = window.setTimeout(() => {
      setRunHealthPopover(null);
      runHealthPopoverHideTimerRef.current = null;
    }, delayMs);
  }, []);

  const hideRunHealthPopover = useCallback(() => {
    clearRunHealthPopoverTimers();
    setRunHealthPopover(null);
  }, [clearRunHealthPopoverTimers]);

  const runStatuses = useMemo(() => {
    const unique = new Set<string>();
    for (const run of sortedRuns) {
      unique.add(normalizeStatus(run.status));
    }
    return Array.from(unique).sort((left, right) => statusRank(left) - statusRank(right) || left.localeCompare(right));
  }, [sortedRuns]);

  const jobLabelByKey = useMemo(() => {
    const labels: Record<string, string> = {};
    for (const job of jobs) {
      labels[job.job_key] = job.display_name || job.job_key;
    }
    return labels;
  }, [jobs]);

  const runsQuickCounts = useMemo(() => {
    const counts: Record<RunsQuickFilter, number> = {
      all: sortedRuns.length,
      backfills: 0,
      queued: 0,
      in_progress: 0,
      failed: 0,
      scheduled: 0,
    };
    for (const run of sortedRuns) {
      if (matchesRunQuickFilter(run, "backfills")) counts.backfills += 1;
      if (matchesRunQuickFilter(run, "queued")) counts.queued += 1;
      if (matchesRunQuickFilter(run, "in_progress")) counts.in_progress += 1;
      if (matchesRunQuickFilter(run, "failed")) counts.failed += 1;
      if (matchesRunQuickFilter(run, "scheduled")) counts.scheduled += 1;
    }
    return counts;
  }, [sortedRuns]);

  const runsFilterText = runsSearch.trim().toLowerCase();
  const filteredRuns = useMemo(() => {
    let rows = sortedRuns;
    if (runsQuickFilter !== "all") {
      rows = rows.filter((run) => matchesRunQuickFilter(run, runsQuickFilter));
    }
    if (runsJobFilter !== "all") {
      rows = rows.filter((run) => run.job_key === runsJobFilter);
    }
    if (runsStatusFilter !== "all") {
      rows = rows.filter((run) => normalizeStatus(run.status) === runsStatusFilter);
    }
    if (runsWindowHours > 0) {
      const threshold = Date.now() - runsWindowHours * 60 * 60 * 1000;
      rows = rows.filter((run) => runTimestampMs(run) >= threshold);
    }
    if (runsFilterText) {
      rows = rows.filter((run) => {
        const haystack = `${run.run_key} ${run.job_key} ${run.triggered_by}`.toLowerCase();
        return haystack.includes(runsFilterText);
      });
    }
    const sorted = [...rows];
    if (runsSort === "oldest") {
      sorted.sort((left, right) => runTimestampMs(left) - runTimestampMs(right) || left.id - right.id);
      return sorted;
    }
    if (runsSort === "duration_desc") {
      sorted.sort((left, right) => runDurationMs(right) - runDurationMs(left) || runTimestampMs(right) - runTimestampMs(left));
      return sorted;
    }
    sorted.sort((left, right) => runTimestampMs(right) - runTimestampMs(left) || right.id - left.id);
    return sorted;
  }, [runsFilterText, runsJobFilter, runsQuickFilter, runsSort, runsStatusFilter, runsWindowHours, sortedRuns]);

  const runsPageCount = Math.max(1, Math.ceil(filteredRuns.length / RUNS_PAGE_SIZE));
  const pagedRuns = useMemo(() => {
    const start = runsPageIndex * RUNS_PAGE_SIZE;
    return filteredRuns.slice(start, start + RUNS_PAGE_SIZE);
  }, [filteredRuns, runsPageIndex]);

  const runsPageStart = filteredRuns.length === 0 ? 0 : runsPageIndex * RUNS_PAGE_SIZE + 1;
  const runsPageEnd = Math.min(filteredRuns.length, (runsPageIndex + 1) * RUNS_PAGE_SIZE);

  const applyRoute = useCallback((route: AppRoute) => {
    setActiveSection(route.section);
    if (route.section === "overview") {
      setJobsPage("list");
      setRunsPage("list");
      setRouteJobKey("");
      setRouteRunKey("");
      setSelectedRunID(0);
      return;
    }
    if (route.section === "jobs") {
      setJobsPage(route.jobKey.length > 0 ? "detail" : "list");
      setRunsPage("list");
      setRouteJobKey(route.jobKey);
      setRouteRunKey("");
      setSelectedRunID(0);
      return;
    }
    setRunsPage(route.runKey.length > 0 ? "detail" : "list");
    setJobsPage("list");
    setRouteRunKey(route.runKey);
    setRouteJobKey("");
    setSelectedRunID(0);
  }, []);

  const navigatePath = useCallback(
    (nextPath: string, options?: { replace?: boolean }) => {
      const route = parseRoute(nextPath);
      applyRoute(route);
      const currentPath = normalizePathname(window.location.pathname);
      if (route.path === currentPath) {
        return;
      }
      if (options?.replace) {
        window.history.replaceState(null, "", route.path);
      } else {
        window.history.pushState(null, "", route.path);
      }
    },
    [applyRoute],
  );

  useEffect(() => {
    let active = true;
    const route = parseRoute(window.location.pathname);
    applyRoute(route);
    if (route.path !== normalizePathname(window.location.pathname)) {
      window.history.replaceState(null, "", route.path);
    }
    const onPopState = () => {
      applyRoute(parseRoute(window.location.pathname));
    };
    window.addEventListener("popstate", onPopState);
    void (async () => {
      await Promise.all([refreshJobs(), refreshSchedules(), refreshAllRuns()]);
      if (!active) {
        return;
      }
      markRefreshCycle(Date.now());
    })();
    return () => {
      active = false;
      window.removeEventListener("popstate", onPopState);
    };
  }, []);

  const setTheme = useCallback((nextTheme: ThemeMode) => {
    setThemeMode(nextTheme);
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, nextTheme);
    } catch {
      // Ignore storage errors in restricted environments.
    }
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => {
      setRefreshClockMs(Date.now());
    }, 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", themeMode);
    document.documentElement.style.colorScheme = themeMode;
  }, [themeMode]);

  useEffect(() => {
    const delayMs = Math.max(0, nextAutoRefreshAtMs - Date.now());
    const timer = window.setTimeout(() => {
      if (autoRefreshInFlightRef.current) {
        return;
      }
      autoRefreshInFlightRef.current = true;
      void refreshAllRuns()
        .catch(() => {
          // refreshAllRuns already captures and surfaces errors.
        })
        .finally(() => {
          autoRefreshInFlightRef.current = false;
          markRefreshCycle(Date.now());
        });
    }, delayMs);
    return () => window.clearTimeout(timer);
  }, [nextAutoRefreshAtMs]);

  useEffect(() => {
    if (!overviewFollowNow) {
      return;
    }
    if (overviewLiveShiftMs < TIMELINE_LIVE_REBASE_MS) {
      return;
    }
    setOverviewAnchorMs(refreshClockMs);
  }, [overviewFollowNow, overviewLiveShiftMs, refreshClockMs]);

  useEffect(() => {
    if (activeSection !== "overview") {
      hideTimelineTooltip();
      return;
    }
    const viewport = timelineScrollRef.current;
    if (!viewport) {
      return;
    }
    const measure = () => {
      setTimelineViewportWidth(viewport.clientWidth);
    };
    measure();
    let observer: ResizeObserver | null = null;
    if (typeof ResizeObserver !== "undefined") {
      observer = new ResizeObserver(measure);
      observer.observe(viewport);
    }
    window.addEventListener("resize", measure);
    return () => {
      window.removeEventListener("resize", measure);
      observer?.disconnect();
    };
  }, [activeSection, hideTimelineTooltip]);

  useEffect(() => {
    return () => {
      if (timelineTooltipTimerRef.current !== null) {
        window.clearTimeout(timelineTooltipTimerRef.current);
        timelineTooltipTimerRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    return () => {
      clearRunHealthPopoverTimers();
    };
  }, [clearRunHealthPopoverTimers]);

  useEffect(() => {
    if (activeSection === "jobs" && jobsPage === "list") {
      return;
    }
    hideRunHealthPopover();
  }, [activeSection, hideRunHealthPopover, jobsPage]);

  useEffect(() => {
    if (!runHealthPopover) {
      return;
    }
    const handlePointerDown = (event: MouseEvent) => {
      const target = event.target as Node | null;
      if (target && runHealthPopoverRef.current?.contains(target)) {
        return;
      }
      hideRunHealthPopover();
    };
    window.addEventListener("mousedown", handlePointerDown);
    return () => {
      window.removeEventListener("mousedown", handlePointerDown);
    };
  }, [hideRunHealthPopover, runHealthPopover]);

  useEffect(() => {
    if (!selectedRunID) {
      setRunDetail(null);
      setRunEvents([]);
      setRunEventsTotal(0);
      runEventsScrollToBottomPendingRef.current = false;
      return;
    }
    runEventsScrollToBottomPendingRef.current = true;
    setRunEvents([]);
    setRunEventsTotal(0);
    let active = true;
    let timer: number | null = null;
    const tick = async () => {
      const state = await refreshRunDetail(selectedRunID);
      if (!active) return;
      await refreshRunEvents(selectedRunID);
      if (!active) return;
      if (state === "running") {
        timer = window.setTimeout(() => {
          void tick();
        }, RUN_DETAIL_POLL_INTERVAL_MS);
      }
    };
    void tick();
    return () => {
      active = false;
      if (timer !== null) {
        window.clearTimeout(timer);
      }
    };
  }, [selectedRunID]);

  useEffect(() => {
    if (!selectedJob) {
      if (selectedNodeKey) {
        setSelectedNodeKey("");
      }
      return;
    }
    if (!selectedNodeKey) {
      return;
    }
    if (!dagLayout.nodes.some((node) => node.step_key === selectedNodeKey)) {
      setSelectedNodeKey("");
    }
  }, [dagLayout.nodes, selectedJob, selectedNodeKey]);

  useEffect(() => {
    if (!selectedNodeKey) {
      return;
    }
    if (jobVisibleStepKeys.has(selectedNodeKey)) {
      return;
    }
    setSelectedNodeKey("");
  }, [jobVisibleStepKeys, selectedNodeKey]);

  useEffect(() => {
    if (!routeJobKey) {
      return;
    }
    const matched = jobs.find((job) => job.job_key === routeJobKey);
    if (matched) {
      if (selectedJobKey !== matched.job_key) {
        setSelectedJobKey(matched.job_key);
      }
      return;
    }
    if (jobs.length > 0) {
      setError(`Job ${routeJobKey} was not found`);
      navigatePath("/jobs", { replace: true });
    }
  }, [jobs, navigatePath, routeJobKey, selectedJobKey]);

  useEffect(() => {
    if (!routeRunKey) {
      return;
    }
    const matched = allRuns.find((run) => run.run_key === routeRunKey);
    if (matched) {
      if (selectedRunID !== matched.id) {
        setSelectedRunID(matched.id);
      }
      return;
    }
    if (allRuns.length > 0) {
      setError(`Run ${routeRunKey} was not found`);
      navigatePath("/runs", { replace: true });
    }
  }, [allRuns, navigatePath, routeRunKey, selectedRunID]);

  useEffect(() => {
    if (!runDetail?.summary.job_key) {
      return;
    }
    if (selectedJobKey !== runDetail.summary.job_key) {
      setSelectedJobKey(runDetail.summary.job_key);
    }
  }, [runDetail, selectedJobKey]);

  useEffect(() => {
    if (!runSelectedStepKey) {
      return;
    }
    if (runDagLayout.nodes.some((node) => node.step_key === runSelectedStepKey)) {
      return;
    }
    setRunSelectedStepKey("");
  }, [runDagLayout.nodes, runSelectedStepKey]);

  useEffect(() => {
    if (!selectedRunID || runEvents.length === 0) {
      return;
    }
    const shouldScrollToBottom = runEventsScrollToBottomPendingRef.current || runIsActive;
    if (!shouldScrollToBottom) {
      return;
    }
    const viewport = runEventsViewportRef.current;
    if (!viewport) {
      return;
    }
    window.requestAnimationFrame(() => {
      viewport.scrollTop = viewport.scrollHeight;
      runEventsScrollToBottomPendingRef.current = false;
    });
  }, [runEvents, runIsActive, selectedRunID]);

  useEffect(() => {
    if (runEventStepFilter === "all") {
      return;
    }
    if (!runDagLayout.nodes.some((node) => node.step_key === runEventStepFilter)) {
      setRunEventStepFilter("all");
    }
  }, [runDagLayout.nodes, runEventStepFilter]);

  useEffect(() => {
    setRunHideUnexecuted(false);
    setRunSelectedStepKey("");
    setRunStepGroupFilter("all");
    setCollapsedRunGroups([]);
    setRunEventLevelFilter("all");
    setRunEventStreamFilter("all");
    setRunEventSearch("");
    setRunEventStepFilter("all");
    setCollapsedRunEventIDs([]);
    setRunHoveredStepKey("");
  }, [selectedRunID]);

  useEffect(() => {
    setJobDetailTab("overview");
  }, [selectedJobKey]);

  useEffect(() => {
    const valid = new Set(filteredRunEvents.map((event) => event.id));
    setCollapsedRunEventIDs((current) => current.filter((id) => valid.has(id)));
  }, [filteredRunEvents]);

  useEffect(() => {
    setRunsPageIndex(0);
  }, [runsFilterText, runsJobFilter, runsQuickFilter, runsSort, runsStatusFilter, runsWindowHours]);

  useEffect(() => {
    const maxPage = Math.max(0, runsPageCount - 1);
    if (runsPageIndex > maxPage) {
      setRunsPageIndex(maxPage);
    }
  }, [runsPageCount, runsPageIndex]);

  function toggleRunGroupCollapse(group: RunStepGroupKey) {
    setCollapsedRunGroups((current) => {
      if (current.includes(group)) {
        return current.filter((key) => key !== group);
      }
      return [...current, group];
    });
  }

  function toggleCollapsedRunEvent(eventID: number) {
    setCollapsedRunEventIDs((current) => {
      if (current.includes(eventID)) {
        return current.filter((id) => id !== eventID);
      }
      return [...current, eventID];
    });
  }

  function handleJobNodeSelect(stepKey: string) {
    if (!stepKey) {
      return;
    }
    setSelectedNodeKey((current) => (current === stepKey ? "" : stepKey));
  }

  function handleRunStepSelect(stepKey: string) {
    if (!stepKey) {
      return;
    }
    const deselect = runSelectedStepKey === stepKey;
    setRunSelectedStepKey(deselect ? "" : stepKey);
    setRunEventStepFilter(deselect ? "all" : stepKey);
  }

  async function refreshJobs() {
    try {
      setError("");
      const response = (await api.jobs.JobsGetMany({ limit: 300, offset: 0 })) as JobListResponse;
      if (response.error) {
        throw new Error(response.error);
      }
      const nextJobs = response.data ?? [];
      setJobs(nextJobs);
      if (!selectedJobKey && nextJobs.length > 0) {
        setSelectedJobKey(nextJobs[0].job_key);
      }
      if (selectedJobKey && !nextJobs.some((job) => job.job_key === selectedJobKey)) {
        setSelectedJobKey(nextJobs[0]?.job_key ?? "");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load jobs");
    }
  }

  async function refreshSchedules() {
    try {
      const response = (await api.schedules.SchedulesGetMany({ limit: 300, offset: 0 })) as SchedulesResponse;
      if (response.error) {
        throw new Error(response.error);
      }
      setScheduleRows(response.data ?? []);
    } catch {
      setScheduleRows([]);
    }
  }

  async function refreshAllRuns() {
    try {
      setError("");
      const response = (await api.runs.RunsGetMany({ job_key: "", limit: 1000, offset: 0 })) as RunsListResponse;
      if (response.error) {
        throw new Error(response.error);
      }
      setAllRuns(response.data ?? []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load runs");
    }
  }

  async function refreshRunDetail(runID: number): Promise<"running" | "done"> {
    try {
      const response = (await api.runs.RunByID({ id: runID })) as RunByIDResponse;
      if (response.error) {
        throw new Error(response.error);
      }
      if (response.run) {
        setRunDetail(response.run);
      }
      if (response.run?.summary.status === "running" || response.run?.summary.status === "queued") {
        return "running";
      }
      return "done";
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load run detail");
      return "done";
    }
  }

  async function refreshRunEvents(runID: number): Promise<void> {
    try {
      setRunEventsBusy(true);
      const response = (await api.runs.RunEventsGetMany({
        run_id: runID,
        limit: RUN_EVENTS_PAGE_SIZE,
        offset: 0,
        tail: true,
      })) as RunEventsGetManyResponse;
      if (response.error) {
        throw new Error(response.error);
      }
      const rows = response.data ?? [];
      setRunEvents(rows);
      setRunEventsTotal(response.total ?? rows.length);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load run events");
      setRunEvents([]);
      setRunEventsTotal(0);
    } finally {
      setRunEventsBusy(false);
    }
  }

  async function launchRunForJob(jobKey: string, options?: { openRunOnSuccess?: boolean }) {
    if (!jobKey) return;
    try {
      setRunLaunchPendingByJobKey((current) => ({ ...current, [jobKey]: true }));
      setError("");
      const response = (await api.runs.RunCreate({
        job_key: jobKey,
        triggered_by: "ui",
      })) as RunCreateResponse;
      if (response.error) {
        throw new Error(response.error);
      }
      await refreshAllRuns();
      if (options?.openRunOnSuccess && response.run?.run_key) {
        openRun(response.run.run_key);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to launch run");
    } finally {
      setRunLaunchPendingByJobKey((current) => {
        if (!current[jobKey]) {
          return current;
        }
        const next = { ...current };
        delete next[jobKey];
        return next;
      });
    }
  }

  async function launchRun() {
    if (!selectedJobKey) return;
    await launchRunForJob(selectedJobKey, { openRunOnSuccess: true });
  }

  async function updateJobSchedulingForJob(jobKey: string, schedulingPaused: boolean) {
    if (!jobKey) return;
    try {
      setScheduleUpdatePendingByKey((current) => ({ ...current, [jobKey]: true }));
      setError("");
      const response = (await api.jobs.JobSchedulingUpdate({
        job_key: jobKey,
        scheduling_paused: schedulingPaused,
      })) as JobSchedulingUpdateResponse;
      if (response.error) {
        throw new Error(response.error);
      }
      await Promise.all([refreshJobs(), refreshSchedules(), refreshAllRuns()]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update scheduling state");
    } finally {
      setScheduleUpdatePendingByKey((current) => {
        if (!current[jobKey]) {
          return current;
        }
        const next = { ...current };
        delete next[jobKey];
        return next;
      });
    }
  }

  async function terminateRun() {
    if (!runDetail) return;
    try {
      setLoading(true);
      setError("");
      const response = (await api.runs.RunTerminate({
        id: runDetail.summary.id,
      })) as RunTerminateResponse;
      if (response.error) {
        throw new Error(response.error);
      }
      await Promise.all([refreshAllRuns(), refreshRunDetail(runDetail.summary.id), refreshRunEvents(runDetail.summary.id)]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to terminate run");
    } finally {
      setLoading(false);
    }
  }

  function openRun(runKey: string) {
    if (!runKey) {
      navigatePath("/runs");
      return;
    }
    navigatePath(`/runs/${encodeURIComponent(runKey)}`);
  }

  async function copyRunKey(runKey: string) {
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(runKey);
      }
    } catch {
      // ignore clipboard errors in older browsers or denied permission
    }
  }

  function openJobHistory(jobKey: string) {
    setRunsJobFilter(jobKey);
    navigatePath("/runs");
  }

  function openJobDetail(jobKey: string) {
    const job = jobs.find((entry) => entry.job_key === jobKey);
    if (!job) {
      navigatePath("/jobs");
      return;
    }
    setSelectedJobKey(job.job_key);
    navigatePath(`/jobs/${encodeURIComponent(job.job_key)}`);
  }

  function shiftOverviewWindow(direction: -1 | 1) {
    setOverviewFollowNow(false);
    const delta = Math.floor(overviewWindowMs / 2);
    setOverviewAnchorMs((current) => {
      const base = overviewFollowNow ? refreshClockMs : current;
      const next = base + direction * delta;
      if (direction > 0) {
        return Math.min(next, Date.now());
      }
      return next;
    });
  }

  function markRefreshCycle(referenceMs: number) {
    setRefreshClockMs(referenceMs);
    setLastRefreshAtMs(referenceMs);
    setNextAutoRefreshAtMs(referenceMs + RUNS_POLL_INTERVAL_MS);
  }

  async function refreshFromControl() {
    if (refreshControlBusy || autoRefreshInFlightRef.current) {
      return;
    }
    setRefreshControlBusy(true);
    autoRefreshInFlightRef.current = true;
    try {
      await Promise.all([refreshJobs(), refreshSchedules(), refreshAllRuns()]);
    } finally {
      autoRefreshInFlightRef.current = false;
      setRefreshControlBusy(false);
      markRefreshCycle(Date.now());
    }
  }

  const selectedJobSchedules = useMemo(() => {
    if (!selectedJob) {
      return [] as ScheduleRow[];
    }
    const explicitRows = (scheduleRows ?? []).filter((entry) => entry.job_key === selectedJob.job_key);
    if (explicitRows.length > 0) {
      return explicitRows;
    }
    return (selectedJob.schedules ?? []).map((entry, index) => ({
      id: -(index + 1),
      job_id: selectedJob.id,
      job_key: selectedJob.job_key,
      schedule_key: entry.schedule_key,
      cron_expr: entry.cron_expr,
      timezone: entry.timezone,
      is_enabled: entry.is_enabled,
      description: entry.description,
    }));
  }, [scheduleRows, selectedJob]);
  const selectedJobHasSchedules = selectedJobSchedules.length > 0;

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <button className="sidebar-brand-home" onClick={() => navigatePath("/")} aria-label="Go to overview">
            <img className="sidebar-brand-logo" src={daggoLogoSrc} alt="DAGGO" />
          </button>
          <h1>Ops Console</h1>
          <p className="muted">Strongly typed orchestration observability</p>
        </div>

        <nav className="sidebar-nav">
          {NAV_ITEMS.map((item) => (
            <button
              key={item.key}
              className={`sidebar-link ${activeSection === item.key ? "active" : ""}`}
              onClick={() => {
                if (item.key === "overview") {
                  navigatePath("/");
                  return;
                }
                navigatePath(`/${item.key}`);
              }}
            >
              <strong>{item.label}</strong>
              <span>{item.detail}</span>
            </button>
          ))}
        </nav>

        <div className="sidebar-foot">
          <a href="/rpc/docs/" target="_blank" rel="noreferrer">
            RPC Docs
          </a>
        </div>
      </aside>

      <div className="content-shell">
        <header className="content-topbar">
          <div>
            <h2>{NAV_ITEMS.find((item) => item.key === activeSection)?.label}</h2>
            <p className="muted">
              {activeSection === "overview"
                ? "Temporal execution density and live system pulse"
                : activeSection === "jobs"
                  ? "Structural inventory of executable units and schedules"
                  : "Run-centric history, filtering, and step-level diagnostics"}
            </p>
          </div>
          <div className="content-topbar-controls">
            <div className="theme-toggle" role="group" aria-label="Theme mode">
              <button
                type="button"
                className={`theme-toggle-option ${themeMode === "dark" ? "active" : ""}`}
                aria-pressed={themeMode === "dark"}
                onClick={() => setTheme("dark")}
              >
                Dark
              </button>
              <button
                type="button"
                className={`theme-toggle-option ${themeMode === "light" ? "active" : ""}`}
                aria-pressed={themeMode === "light"}
                onClick={() => setTheme("light")}
              >
                Light
              </button>
            </div>
            <div
              className={`refresh-indicator ${refreshJustUpdated ? "pulse" : ""}`}
              title={`Auto-refresh every ${Math.floor(RUNS_POLL_INTERVAL_MS / 1000)} seconds`}
            >
              <span className="refresh-countdown" aria-live="polite">
                {refreshCountdownLabel}
              </span>
              <button
                type="button"
                className={`refresh-icon-btn ${refreshControlBusy ? "busy" : ""}`}
                onClick={() => void refreshFromControl()}
                disabled={refreshControlBusy}
                aria-label="Refresh now"
                title={`Auto-refresh every ${Math.floor(RUNS_POLL_INTERVAL_MS / 1000)} seconds`}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path
                    fill="currentColor"
                    d="M17.8 6.2a8 8 0 1 0 2.1 8.2h-2.1a6 6 0 1 1-1.7-6.1L13.5 11H22V2.5l-4.2 3.7z"
                  />
                </svg>
              </button>
            </div>
          </div>
        </header>

        {error && <div className="alert">{error}</div>}

        <main className="view-root">
          {activeSection === "overview" ? (
            <section className="panel">
              <div className="panel-head">
                <h3>Execution Timeline</h3>
                <span className={`pill ${anchoredToNow ? "running" : "pending"}`}>
                  {anchoredToNow ? "anchored to now" : "historical slice"}
                </span>
              </div>

              <div className="overview-controls">
                <label className="filter-field">
                  <span>Filter Jobs</span>
                  <input
                    value={overviewQuery}
                    onChange={(event) => setOverviewQuery(event.target.value)}
                    placeholder="Search job key or name"
                  />
                </label>

                <div className="window-picker">
                  {OVERVIEW_WINDOWS.map((hours) => (
                    <button
                      key={hours}
                      className={`ghost-btn ${overviewWindowHours === hours ? "active" : ""}`}
                      onClick={() => setOverviewWindowHours(hours)}
                    >
                      {hours}h
                    </button>
                  ))}
                </div>

                <div className="time-nav">
                  <button className="ghost-btn" onClick={() => shiftOverviewWindow(-1)}>
                    ◀
                  </button>
                  <button className="ghost-btn" onClick={() => shiftOverviewWindow(1)}>
                    ▶
                  </button>
                  <button
                    className="ghost-btn"
                    onClick={() => {
                      const now = Date.now();
                      setOverviewFollowNow(true);
                      setOverviewAnchorMs(now);
                      setRefreshClockMs(now);
                    }}
                  >
                    Now
                  </button>
                </div>
              </div>

              <div className="overview-range">
                <strong>
                  {formatTsShort(overviewDisplayStartMs)} - {formatTsShort(overviewDisplayEndMs)}
                </strong>
                <span className="muted">{overviewStats.totalRunsInWindow} runs in selected window</span>
              </div>

              <div className="overview-legend">
                <span className="legend-dot failed" />
                <span>Failed</span>
                <span className="legend-dot succeeded" />
                <span>Succeeded</span>
                <span className="legend-dot running" />
                <span>Running</span>
                <span className="legend-dot will_run" />
                <span>Will run (scheduled)</span>
              </div>

              <div className="overview-kpis">
                <article className="stat-card">
                  <strong>{overviewStats.runningNow}</strong>
                  <span>Running / queued now</span>
                </article>
                <article className="stat-card">
                  <strong>{overviewStats.failedInWindow}</strong>
                  <span>Failures in window</span>
                </article>
                <article className="stat-card">
                  <strong>{overviewStats.successInWindow}</strong>
                  <span>Successes in window</span>
                </article>
                <article className="stat-card">
                  <strong>{overviewStats.enabledSchedules}</strong>
                  <span>Enabled schedules</span>
                </article>
                <article className="stat-card">
                  <strong>{overviewStats.quietJobs}</strong>
                  <span>Jobs with no runs</span>
                </article>
              </div>

              <div className="timeline-wrap">
                <div className="timeline-scroll" ref={timelineScrollRef} onScroll={hideTimelineTooltip}>
                  <div className="timeline-canvas" style={{ width: `${timelineTrackWidth + timelineLabelWidth}px` }}>
                    <div className="timeline-axis-row">
                      <div className="timeline-axis-label">Jobs</div>
                      <div className="timeline-axis-track" style={{ width: `${timelineTrackWidth}px` }}>
                        <div className="timeline-date-band">
                          <div className="timeline-moving-layer" style={timelineShiftStyle}>
                            {overviewDateMarkers.map((marker) => (
                              <span
                                key={marker.key}
                                className={`timeline-date-marker ${marker.start ? "start" : ""}`}
                                style={{ left: marker.left }}
                              >
                                {marker.label}
                              </span>
                            ))}
                          </div>
                        </div>
                        <div className="timeline-time-band">
                          <span className="timeline-future-region" style={{ left: `${overviewNowLeftPct}%` }} />
                          <span className="timeline-now-cursor axis" style={{ left: `${overviewNowLeftPct}%` }}>
                            <em>Now</em>
                          </span>
                          <div className="timeline-moving-layer" style={timelineShiftStyle}>
                            {overviewTicks.map((tick) => (
                              <div key={tick.key} className="timeline-tick" style={{ left: `${tick.leftPct}%` }}>
                                <span>{tick.timeLabel}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      </div>
                    </div>

                    {overviewJobs.map((job) => {
                      const artifacts = overviewArtifactsByJob.get(job.job_key) ?? [];
                      return (
                        <div key={job.job_key} className="timeline-row">
                          <button
                            type="button"
                            className="timeline-job-label timeline-job-link"
                            onClick={() => openJobDetail(job.job_key)}
                            aria-label={`Open job ${job.display_name || job.job_key}`}
                          >
                            <strong>{job.display_name || job.job_key}</strong>
                            <span>{job.job_key}</span>
                          </button>

                          <div className="timeline-track" style={{ width: `${timelineTrackWidth}px` }}>
                            <span className="timeline-future-region" style={{ left: `${overviewNowLeftPct}%` }} />
                            <span className="timeline-now-cursor" style={{ left: `${overviewNowLeftPct}%` }} />
                            <div className="timeline-moving-layer" style={timelineShiftStyle}>
                              {overviewTicks.map((tick) => (
                                <span key={`${job.job_key}-${tick.key}`} className="timeline-grid-line" style={{ left: `${tick.leftPct}%` }} />
                              ))}

                              {artifacts.map((artifact) => {
                                const layout = computeTimelineMarkerLayout({
                                  artifact,
                                  windowStartMs: overviewStartMs,
                                  windowMs: overviewRangeMs,
                                  trackWidthPx: timelineTrackWidth,
                                  nowMs: refreshClockMs,
                                });
                                return (
                                  <button
                                    key={artifact.id}
                                    className={`timeline-marker ${artifact.state}`}
                                    style={{ left: `${layout.leftPx}px`, width: `${layout.widthPx}px`, zIndex: layout.zIndex }}
                                    onMouseEnter={(event) => queueTimelineTooltip(event.currentTarget, artifact.tooltip)}
                                    onMouseLeave={hideTimelineTooltip}
                                    onFocus={(event) => queueTimelineTooltip(event.currentTarget, artifact.tooltip)}
                                    onBlur={hideTimelineTooltip}
                                    onClick={() => {
                                      hideTimelineTooltip();
                                      if (artifact.runKey) {
                                        openRun(artifact.runKey);
                                        return;
                                      }
                                      openJobDetail(job.job_key);
                                    }}
                                    aria-label={artifact.tooltip}
                                  />
                                );
                              })}
                            </div>
                          </div>
                        </div>
                      );
                    })}

                    {timelineTooltip ? (
                      <div className="timeline-tooltip" style={{ left: `${timelineTooltip.x}px`, top: `${timelineTooltip.y}px` }}>
                        {timelineTooltip.text}
                      </div>
                    ) : null}

                    {overviewJobs.length === 0 ? <p className="muted timeline-empty">No jobs match this filter.</p> : null}
                  </div>
                </div>
              </div>
            </section>
          ) : null}

          {activeSection === "jobs" ? (
            <div className="jobs-page-shell">
              {jobsPage === "list" ? (
                <section className="panel jobs-panel jobs-table-panel">
                  <div className="panel-head jobs-table-head">
                    <h3>Jobs</h3>
                    <span className="muted">
                      {jobsForList.length} shown / {jobs.length} total
                    </span>
                  </div>

                  <div className="jobs-table-controls">
                    <label className="jobs-search-field">
                      <span>Search</span>
                      <input
                        value={jobsQuery}
                        onChange={(event) => setJobsQuery(event.target.value)}
                        placeholder="Filter by job name, key, or description"
                      />
                    </label>
                  </div>

                  <div className="jobs-table-wrap">
                    <table className="jobs-table">
                      <colgroup>
                        <col className="jobs-col-active" />
                        <col className="jobs-col-job" />
                        <col className="jobs-col-last-run" />
                        <col className="jobs-col-health" />
                        <col className="jobs-col-next-run" />
                        <col className="jobs-col-actions" />
                      </colgroup>
                      <thead>
                        <tr>
                          <th className="jobs-col-active">Active</th>
                          <th className="jobs-col-job">Job</th>
                          <th className="jobs-col-last-run">Last Run</th>
                          <th className="jobs-col-health">Run Health</th>
                          <th className="jobs-col-next-run">Next Run</th>
                          <th className="jobs-col-actions">Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {jobsForList.map((job) => {
                          const latestRun = latestRunByJob[job.job_key] ?? null;
                          const recentRuns = recentRunsByJob[job.job_key] ?? [];
                          const persistedSchedules = (scheduleRowsByJob[job.job_key] ?? []).map((row) => ({
                            schedule_key: row.schedule_key,
                            cron_expr: row.cron_expr,
                            timezone: row.timezone,
                            is_enabled: row.is_enabled,
                            description: row.description,
                          }));
                          const fallbackSchedules = (job.schedules ?? []).map((row) => ({
                            schedule_key: row.schedule_key,
                            cron_expr: row.cron_expr,
                            timezone: row.timezone,
                            is_enabled: row.is_enabled,
                            description: row.description,
                          }));
                          const scheduleEntries = persistedSchedules.length > 0 ? persistedSchedules : fallbackSchedules;
                          const hasSchedules = scheduleEntries.length > 0;
                          const enabledSchedules = scheduleEntries.filter((entry) => entry.is_enabled);
                          const schedulingEnabled = hasSchedules && !job.scheduling_paused && enabledSchedules.length > 0;
                          const nextSchedule = enabledSchedules[0] ?? scheduleEntries[0] ?? null;
                          const nextRunLabel = !nextSchedule
                            ? "Not scheduled"
                            : !schedulingEnabled
                              ? "Paused"
                              : formatCronHuman(nextSchedule.cron_expr, nextSchedule.timezone);
                          const latestRunTimestamp = latestRun
                            ? latestRun.started_at || latestRun.queued_at || latestRun.completed_at
                            : "";
                          const disablePending = Boolean(scheduleUpdatePendingByKey[job.job_key]);
                          const launchPending = Boolean(runLaunchPendingByJobKey[job.job_key]);
                          return (
                            <tr key={job.job_key}>
                              <td className="jobs-cell-active">
                                <label className={`job-active-toggle ${!hasSchedules ? "disabled" : ""}`}>
                                  <input
                                    type="checkbox"
                                    checked={schedulingEnabled}
                                    disabled={disablePending || !hasSchedules}
                                    aria-label={`Toggle ${job.display_name || job.job_key} scheduling`}
                                    onChange={(event) => {
                                      const nextEnabled = event.target.checked;
                                      if (!nextEnabled) {
                                        const confirmed = window.confirm(
                                          `Disable scheduling for ${job.display_name || job.job_key}?`,
                                        );
                                        if (!confirmed) {
                                          return;
                                        }
                                      }
                                      void updateJobSchedulingForJob(job.job_key, !nextEnabled);
                                    }}
                                  />
                                  <span className="job-active-slider" />
                                </label>
                              </td>
                              <td className="jobs-cell-job">
                                <button className="job-link-btn" onClick={() => openJobDetail(job.job_key)}>
                                  {job.display_name || job.job_key}
                                </button>
                                <small>{job.job_key}</small>
                              </td>
                              <td className="jobs-cell-last-run">
                                <span>{latestRun ? formatRelativeTimeFromNow(latestRunTimestamp, refreshClockMs) : "Never ran"}</span>
                                <small>{latestRun ? formatTs(latestRunTimestamp) : "No runs yet"}</small>
                              </td>
                              <td className="jobs-cell-health">
                                <div className="job-health-histogram" aria-label={`Recent runs for ${job.display_name || job.job_key}`}>
                                  {Array.from({ length: JOB_HEALTH_RUN_COUNT }).map((_, index) => {
                                    const runSlotIndex = JOB_HEALTH_RUN_COUNT - 1 - index;
                                    const run = recentRuns[runSlotIndex] ?? null;
                                    const status = run ? normalizeStatus(run.status) : "empty";
                                    const runTs = run ? run.started_at || run.queued_at || run.completed_at : "";
                                    const tileTitle = run
                                      ? `${runStatusLabel(status)} | ${formatTs(runTs)} | ${formatRunDurationCell(run)}`
                                      : "Run details unavailable";
                                    if (!run) {
                                      return <span key={`${job.job_key}-${index}`} className={`job-health-tile ${status}`} title="No run" />;
                                    }
                                    return (
                                      <button
                                        key={`${job.job_key}-${index}`}
                                        type="button"
                                        className={`job-health-tile ${status}`}
                                        title={tileTitle}
                                        aria-label={tileTitle}
                                        onMouseEnter={(event) =>
                                          queueRunHealthPopover(event.currentTarget, run, job.display_name || job.job_key)
                                        }
                                        onMouseLeave={() => scheduleRunHealthPopoverHide()}
                                        onFocus={(event) =>
                                          queueRunHealthPopover(event.currentTarget, run, job.display_name || job.job_key)
                                        }
                                        onBlur={() => scheduleRunHealthPopoverHide()}
                                      />
                                    );
                                  })}
                                </div>
                              </td>
                              <td className="jobs-cell-next-run">
                                <span>{nextRunLabel}</span>
                                {enabledSchedules.length > 0 ? <small>{enabledSchedules.length} enabled</small> : null}
                              </td>
                              <td className="jobs-cell-actions">
                                <details className="job-actions-menu">
                                  <summary aria-label={`Actions for ${job.display_name || job.job_key}`}>•••</summary>
                                  <div className="job-actions-popover">
                                    <button type="button" onClick={() => openJobHistory(job.job_key)}>
                                      View Runs
                                    </button>
                                    <button
                                      type="button"
                                      disabled={launchPending}
                                      onClick={() => void launchRunForJob(job.job_key)}
                                    >
                                      {launchPending ? "Submitting..." : "Trigger Run"}
                                    </button>
                                  </div>
                                </details>
                              </td>
                            </tr>
                          );
                        })}
                        {jobsForList.length === 0 ? (
                          <tr>
                            <td className="jobs-empty-row" colSpan={6}>
                              No jobs match the current filter.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </section>
              ) : (
                <section className="panel graph-panel detail-page job-detail-page">
                  {!selectedJob ? (
                    <p className="job-empty-state">Select a job from the list to inspect structure.</p>
                  ) : (
                    <>
                      <div className="panel-head detail-top">
                        <div className="job-detail-tabs" role="tablist" aria-label="Job detail tabs">
                          <button
                            role="tab"
                            id="job-detail-tab-overview"
                            aria-controls="job-detail-panel-overview"
                            aria-selected={jobDetailTab === "overview"}
                            className={`job-detail-tab ${jobDetailTab === "overview" ? "active" : ""}`}
                            onClick={() => setJobDetailTab("overview")}
                          >
                            Overview
                          </button>
                          <button
                            role="tab"
                            id="job-detail-tab-runs"
                            aria-controls="job-detail-panel-runs"
                            aria-selected={jobDetailTab === "runs"}
                            className={`job-detail-tab ${jobDetailTab === "runs" ? "active" : ""}`}
                            onClick={() => setJobDetailTab("runs")}
                          >
                            Runs
                          </button>
                        </div>
                        <button disabled={runLaunchBusy} className="primary-btn job-run-action-btn" onClick={() => void launchRun()}>
                          {runLaunchBusy ? "Submitting..." : "Run Job"}
                        </button>
                      </div>

                      <header className="job-detail-header">
                        <div className="job-header-primary">
                          <strong>{selectedJob.job_key}</strong>
                          <div className={`job-scheduling-switch ${!selectedJobHasSchedules ? "disabled" : ""}`}>
                            <span className="job-scheduling-switch-label">Enabled</span>
                            <label className={`job-active-toggle ${!selectedJobHasSchedules ? "disabled" : ""}`}>
                              <input
                                type="checkbox"
                                checked={selectedJobHasSchedules && !selectedJob.scheduling_paused}
                                disabled={Boolean(scheduleUpdatePendingByKey[selectedJob.job_key]) || !selectedJobHasSchedules}
                                onChange={(event) => {
                                  const nextEnabled = event.target.checked;
                                  if (!nextEnabled) {
                                    const confirmed = window.confirm(
                                      `Disable scheduling for ${selectedJob.display_name || selectedJob.job_key}?`,
                                    );
                                    if (!confirmed) {
                                      return;
                                    }
                                  }
                                  void updateJobSchedulingForJob(selectedJob.job_key, !nextEnabled);
                                }}
                              />
                              <span className="job-active-slider" />
                            </label>
                            {!selectedJobHasSchedules ? <small className="job-scheduling-switch-state">No schedules</small> : null}
                            {selectedJobHasSchedules && selectedJob.scheduling_paused ? (
                              <small className="job-scheduling-switch-state">Paused</small>
                            ) : null}
                          </div>
                        </div>
                        <p className="job-description job-description-chip">{selectedJob.description || "No job description."}</p>
                        <div className="job-header-chips">
                          <div className="job-header-chip">
                            <span>Steps</span>
                            <strong>{selectedJob.nodes.length}</strong>
                          </div>
                          <div className="job-header-chip">
                            <span>Schedules</span>
                            <strong>{selectedJobSchedules.length}</strong>
                          </div>
                          <div className="job-header-chip">
                            <span>Runs</span>
                            <strong>{selectedJobRunCount}</strong>
                          </div>
                        </div>
                      </header>

                      {jobDetailTab === "overview" ? (
                        <div
                          className="job-detail-overview"
                          role="tabpanel"
                          id="job-detail-panel-overview"
                          aria-labelledby="job-detail-tab-overview"
                        >
                          <div className="detail-grid job-detail-grid">
                            <div className="schedule-box inline-section job-control-card">
                              <h4>Schedules</h4>
                              <ul className="job-schedule-list">
                                {selectedJobSchedules.map((entry) => {
                                  const humanLabel = formatCronHuman(entry.cron_expr, entry.timezone);
                                  const rawLabel = formatCronRaw(entry.cron_expr, entry.timezone);
                                  const description = entry.description?.trim() ?? "";
                                  const primaryLabel =
                                    description && !scheduleTextsEquivalent(description, humanLabel) ? description : humanLabel || "Scheduled run";
                                  return (
                                    <li key={entry.id}>
                                      <span>{primaryLabel}</span>
                                      {rawLabel && !scheduleTextsEquivalent(primaryLabel, rawLabel) ? <code>{rawLabel}</code> : null}
                                    </li>
                                  );
                                })}
                                {selectedJobSchedules.length === 0 ? <li className="job-empty-inline">No schedules configured.</li> : null}
                              </ul>
                            </div>
                          </div>

                          <div className="job-ops-layout">
                            <section className="dag-surface job-dag-pane">
                              <div className="dag-legend job-dag-controls">
                                <span className="job-dag-hint">Click a node to focus dependencies. Click again to clear.</span>
                              </div>

                              <div className="dag-stage-wrap">
                                <DagGraphCanvasHtml
                                  layoutKey={`job-${selectedJob.job_key}`}
                                  themeMode={themeMode}
                                  worldWidth={dagLayout.width}
                                  worldHeight={dagLayout.height}
                                  nodes={jobVisibleNodes}
                                  edges={jobVisibleEdges}
                                  dependenciesByKey={dagLayout.dependenciesByKey}
                                  dependentsByKey={dagLayout.dependentsByKey}
                                  selectedNodeId={selectedNodeKey}
                                  totalNodeCount={selectedJob.nodes.length}
                                  hardCap={DAG_NODE_HARD_CAP}
                                  getNodeClassName={(node) => {
                                    const active = node.step_key === selectedNodeKey;
                                    const dimmed = selectedNodeKey.length > 0 && !relatedNodeSet.has(node.step_key);
                                    return `dag-node node-neutral ${active ? "active" : ""} ${dimmed ? "dimmed" : ""}`;
                                  }}
                                  renderNodeContent={(node) => (
                                    <>
                                      <span className="dag-node-title">{node.display_name || node.step_key}</span>
                                      <span className="pill neutral">definition</span>
                                    </>
                                  )}
                                  onNodeClick={(node) => handleJobNodeSelect(node.step_key)}
                                  emptyMessage="No steps match this filter."
                                />
                              </div>
                            </section>

                            <aside className="dag-inspector job-step-sidebar">
                              {!selectedNode ? (
                                <p className="job-empty-state">Select a step to inspect dependencies.</p>
                              ) : (
                                <>
                                  <div className="panel-head compact">
                                    <h4>{selectedNode.display_name || selectedNode.step_key}</h4>
                                    <span className="pill neutral">definition</span>
                                  </div>
                                  <p className="job-step-description">{selectedNode.description || "No step description."}</p>
                                  <div className="dag-relations">
                                    <div>
                                      <strong>Upstream</strong>
                                      <div className="dag-key-list">
                                        {selectedNodeDependencies.length === 0 ? (
                                          <span className="job-empty-inline">none</span>
                                        ) : (
                                          selectedNodeDependencies.map((key) => <code key={key}>{key}</code>)
                                        )}
                                      </div>
                                    </div>
                                    <div>
                                      <strong>Downstream</strong>
                                      <div className="dag-key-list">
                                        {selectedNodeDependents.length === 0 ? (
                                          <span className="job-empty-inline">none</span>
                                        ) : (
                                          selectedNodeDependents.map((key) => <code key={key}>{key}</code>)
                                        )}
                                      </div>
                                    </div>
                                  </div>
                                </>
                              )}
                            </aside>
                          </div>
                        </div>
                      ) : (
                        <section
                          className="job-runs-list job-runs-tab-panel"
                          role="tabpanel"
                          id="job-detail-panel-runs"
                          aria-labelledby="job-detail-tab-runs"
                        >
                          <div className="panel-head compact">
                            <h4>Recent Runs</h4>
                            <button className="ghost-btn tiny" onClick={() => openJobHistory(selectedJob.job_key)}>
                              View All Runs
                            </button>
                          </div>
                          <div className="job-runs-table-wrap">
                            <table className="job-runs-table">
                              <thead>
                                <tr>
                                  <th>Run ID</th>
                                  <th>Status</th>
                                  <th>Triggered By</th>
                                  <th>Started</th>
                                  <th>Duration</th>
                                  <th>Actions</th>
                                </tr>
                              </thead>
                              <tbody>
                                {selectedJobRuns.map((run) => {
                                  const status = normalizeStatus(run.status);
                                  const startedAt = formatTs(run.started_at || run.queued_at || run.completed_at);
                                  return (
                                    <tr key={run.id} className="job-run-row" onClick={() => openRun(run.run_key)}>
                                      <td className="job-run-id">{run.run_key}</td>
                                      <td>
                                        <span className={`run-status-badge ${status}`}>{runStatusLabel(status)}</span>
                                      </td>
                                      <td className="job-run-trigger">{run.triggered_by || "-"}</td>
                                      <td className="job-run-time">{startedAt}</td>
                                      <td className="job-run-duration">{formatRunDurationCell(run)}</td>
                                      <td className="job-run-actions">
                                        <button
                                          className="ghost-btn tiny"
                                          onClick={(event) => {
                                            event.stopPropagation();
                                            openRun(run.run_key);
                                          }}
                                        >
                                          View
                                        </button>
                                      </td>
                                    </tr>
                                  );
                                })}
                                {selectedJobRuns.length === 0 ? (
                                  <tr>
                                    <td className="job-runs-empty" colSpan={6}>
                                      No runs yet for this job.
                                    </td>
                                  </tr>
                                ) : null}
                              </tbody>
                            </table>
                          </div>
                        </section>
                      )}
                    </>
                  )}
                </section>
              )}
            </div>
          ) : null}

          {activeSection === "runs" ? (
            <div className="runs-page-shell">
              {runsPage === "list" ? (
                <section className="panel runs-list-panel">
                  <div className="panel-head runs-head">
                    <h3>Runs</h3>
                    <span className="muted">{filteredRuns.length} shown</span>
                  </div>

                  <div className="runs-quick-tabs">
                    {RUNS_QUICK_FILTER_ITEMS.map((item) => (
                      <button
                        key={item.key}
                        className={`quick-filter-tab ${runsQuickFilter === item.key ? "active" : ""}`}
                        onClick={() => setRunsQuickFilter(item.key)}
                      >
                        <span>{item.label}</span>
                        <small>{runsQuickCounts[item.key]}</small>
                      </button>
                    ))}
                  </div>

                  <div className="runs-filter-bar">
                    <span className="runs-filter-label">Filter</span>

                    <label>
                      Job
                      <select value={runsJobFilter} onChange={(event) => setRunsJobFilter(event.target.value)}>
                        <option value="all">All jobs</option>
                        {jobs.map((job) => (
                          <option key={job.job_key} value={job.job_key}>
                            {job.display_name || job.job_key}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label>
                      Status
                      <select value={runsStatusFilter} onChange={(event) => setRunsStatusFilter(event.target.value)}>
                        <option value="all">All statuses</option>
                        {runStatuses.map((status) => (
                          <option key={status} value={status}>
                            {runStatusLabel(status)}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label>
                      Time
                      <select value={runsWindowHours} onChange={(event) => setRunsWindowHours(Number(event.target.value) as RunsWindow)}>
                        {RUNS_WINDOWS.map((hours) => (
                          <option key={hours} value={hours}>
                            {hours === 0 ? "All time" : `Last ${hours}h`}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label>
                      Sort
                      <select value={runsSort} onChange={(event) => setRunsSort(event.target.value as RunsSort)}>
                        {RUNS_SORT_ITEMS.map((sortOption) => (
                          <option key={sortOption.key} value={sortOption.key}>
                            {sortOption.label}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label className="runs-search-field">
                      Search
                      <input value={runsSearch} onChange={(event) => setRunsSearch(event.target.value)} placeholder="run key or trigger" />
                    </label>
                  </div>

                  <div className="runs-table-wrap">
                    <table className="runs-table">
                      <thead>
                        <tr>
                          <th>Run ID</th>
                          <th>Job</th>
                          <th>Triggered By</th>
                          <th>Status</th>
                          <th>Created At</th>
                          <th>Duration</th>
                        </tr>
                      </thead>
                      <tbody>
                        {pagedRuns.map((run) => {
                          const status = normalizeStatus(run.status);
                          const createdAt = formatTs(run.queued_at || run.started_at || run.completed_at);
                          return (
                            <tr key={run.id} className="runs-row" onClick={() => openRun(run.run_key)}>
                              <td className="cell-run-id">
                                <span className="run-id">{run.run_key}</span>
                                <button
                                  className="run-id-copy-btn"
                                  aria-label={`Copy run id ${run.run_key}`}
                                  title="Copy run id"
                                  onClick={(event) => {
                                    event.stopPropagation();
                                    void copyRunKey(run.run_key);
                                  }}
                                >
                                  <svg viewBox="0 0 16 16" aria-hidden="true">
                                    <rect x="5" y="3" width="8" height="10" rx="1.5" />
                                    <rect x="2" y="6" width="8" height="8" rx="1.5" />
                                  </svg>
                                </button>
                              </td>
                              <td>
                                <span className="run-job-chip">{jobLabelByKey[run.job_key] ?? run.job_key}</span>
                              </td>
                              <td className="cell-trigger">{run.triggered_by || "-"}</td>
                              <td>
                                <span className={`run-status-badge ${status}`}>{runStatusLabel(status)}</span>
                              </td>
                              <td className="cell-created">{createdAt}</td>
                              <td className="cell-duration">{formatRunDurationCell(run)}</td>
                            </tr>
                          );
                        })}
                        {pagedRuns.length === 0 ? (
                          <tr>
                            <td className="runs-empty-row" colSpan={6}>
                              No runs match the current filters.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>

                  <div className="runs-pagination">
                    <span className="muted">
                      {runsPageStart}-{runsPageEnd} of {filteredRuns.length}
                    </span>
                    <div className="runs-pagination-controls">
                      <button className="ghost-btn tiny" disabled={runsPageIndex === 0} onClick={() => setRunsPageIndex(0)}>
                        First
                      </button>
                      <button
                        className="ghost-btn tiny"
                        disabled={runsPageIndex === 0}
                        onClick={() => setRunsPageIndex((current) => Math.max(0, current - 1))}
                      >
                        Prev
                      </button>
                      <span className="runs-page-label">
                        Page {Math.min(runsPageIndex + 1, runsPageCount)} / {runsPageCount}
                      </span>
                      <button
                        className="ghost-btn tiny"
                        disabled={runsPageIndex >= runsPageCount - 1}
                        onClick={() => setRunsPageIndex((current) => Math.min(runsPageCount - 1, current + 1))}
                      >
                        Next
                      </button>
                    </div>
                  </div>
                </section>
              ) : (
                <section className="panel details-panel detail-page run-detail-page">
                  <div className="panel-head detail-top">
                    <button className="ghost-btn tiny" onClick={() => navigatePath("/runs")}>
                      Back to Runs
                    </button>
                    {runDetail && (normalizeStatus(runDetail.summary.status) === "running" || normalizeStatus(runDetail.summary.status) === "queued") ? (
                      <button className="ghost-btn tiny danger-outline" disabled={loading} onClick={() => void terminateRun()}>
                        {loading ? "Terminating..." : "Terminate Run"}
                      </button>
                    ) : null}
                    <button
                      className="ghost-btn tiny"
                      onClick={() => {
                        setRunHideUnexecuted(false);
                        setRunStepGroupFilter("all");
                        setRunEventLevelFilter("all");
                        setRunEventStreamFilter("all");
                        setRunEventSearch("");
                        setRunEventStepFilter("all");
                      }}
                    >
                      Clear Filters
                    </button>
                  </div>

                  {!runDetail ? (
                    <p className="muted">Select a run to inspect steps and logs.</p>
                  ) : (
                    <>
                      <header className="run-detail-header">
                        <div className="run-header-primary">
                          <strong>{runDetail.summary.run_key}</strong>
                          <span className={`run-status-badge ${normalizeStatus(runDetail.summary.status)}`}>
                            {runStatusLabel(runDetail.summary.status)}
                          </span>
                          <span className="run-job-chip">{jobLabelByKey[runDetail.summary.job_key] ?? runDetail.summary.job_key}</span>
                        </div>
                        <div className="run-header-meta">
                          <div>
                            <span>Launched By</span>
                            <strong>{runDetail.summary.triggered_by || "-"}</strong>
                          </div>
                          <div>
                            <span>Created At</span>
                            <strong>{formatTs(runDetail.summary.queued_at)}</strong>
                          </div>
                          <div>
                            <span>Started</span>
                            <strong>{formatTs(runDetail.summary.started_at)}</strong>
                          </div>
                          <div>
                            <span>Duration</span>
                            <strong>{runDurationLabel(runDetail.summary) || "-"}</strong>
                          </div>
                        </div>
                      </header>

                      {runDetail.summary.error_message ? <div className="run-error-banner">{runDetail.summary.error_message}</div> : null}

                      <div className="run-ops-layout">
                        <section className="run-graph-pane">
                          <div className="run-graph-controls">
                            <label className="run-inline-check">
                              <input
                                type="checkbox"
                                checked={runHideUnexecuted}
                                onChange={(event) => setRunHideUnexecuted(event.target.checked)}
                              />
                              Hide unexecuted
                            </label>
                            <span className="run-graph-hint">Click a step to focus upstream/downstream flow. Click again to clear.</span>
                          </div>

                          <div className="run-dag-wrap">
                            <DagGraphCanvasHtml
                              layoutKey={`run-${runDetail.summary.run_key}`}
                              themeMode={themeMode}
                              worldWidth={runDagLayout.width}
                              worldHeight={runDagLayout.height}
                              nodes={runVisibleNodes}
                              edges={runVisibleEdges}
                              dependenciesByKey={runDagLayout.dependenciesByKey}
                              dependentsByKey={runDagLayout.dependentsByKey}
                              selectedNodeId={runSelectedStepKey}
                              totalNodeCount={runDagLayout.nodes.length}
                              hardCap={DAG_NODE_HARD_CAP}
                              getNodeClassName={(node) => {
                                const normalizedStatus = normalizeStatus(stepStatusByKey[node.step_key] ?? "pending");
                                const visual = runStepVisualState(normalizedStatus);
                                const active = node.step_key === runSelectedStepKey;
                                const hovered = node.step_key === runHoveredStepKey;
                                const dimmed = runSelectedStepVisible && !runRelatedNodeSet.has(node.step_key);
                                return `run-step-node ${visual} ${active ? "active" : ""} ${hovered ? "hovered" : ""} ${dimmed ? "dimmed" : ""}`;
                              }}
                              renderNodeContent={(node) => {
                                const normalizedStatus = normalizeStatus(stepStatusByKey[node.step_key] ?? "pending");
                                const step = runStepByKey[node.step_key];
                                const stepMeta = [
                                  step && step.duration_ms > 0 ? formatDurationMsLargest(step.duration_ms) : "",
                                  step && step.attempt > 1 ? `attempt ${step.attempt}` : "",
                                ]
                                  .filter(Boolean)
                                  .join(" | ");
                                return (
                                  <>
                                    <span className="run-step-title">{node.display_name || node.step_key}</span>
                                    <span className="run-step-key">{node.step_key}</span>
                                    <span className="run-step-status">{runStatusLabel(normalizedStatus)}</span>
                                    {stepMeta ? <span className="run-step-meta">{stepMeta}</span> : null}
                                  </>
                                );
                              }}
                              onNodeMouseEnter={(node) => setRunHoveredStepKey(node.step_key)}
                              onNodeMouseLeave={() => setRunHoveredStepKey("")}
                              onNodeClick={(node) => handleRunStepSelect(node.step_key)}
                              emptyMessage="No steps match this filter."
                            />
                          </div>
                        </section>

                        <aside className="run-status-sidebar">
                          <div className="run-status-sidebar-head">
                            <h4>Execution States</h4>
                            <button className="ghost-btn tiny" onClick={() => setRunStepGroupFilter("all")}>
                              All
                            </button>
                          </div>
                          {RUN_STEP_GROUPS.map((group) => {
                            const steps = runStepKeysByGroup[group.key] ?? [];
                            const collapsed = collapsedRunGroups.includes(group.key);
                            const active = runStepGroupFilter === group.key;
                            return (
                              <section key={group.key} className={`run-state-group ${active ? "active" : ""}`}>
                                <button
                                  className="run-state-group-head"
                                  onClick={() => toggleRunGroupCollapse(group.key)}
                                  aria-expanded={!collapsed}
                                >
                                  <span>{group.label}</span>
                                  <strong>{runStepCountsByGroup[group.key] ?? 0}</strong>
                                </button>
                                <div className="run-state-group-tools">
                                  <button
                                    className="ghost-btn tiny"
                                    onClick={() => setRunStepGroupFilter((current) => (current === group.key ? "all" : group.key))}
                                  >
                                    {active ? "Clear" : "Filter"}
                                  </button>
                                </div>
                                {!collapsed ? (
                                  <div className="run-state-step-list">
                                    {steps.slice(0, 8).map((stepKey) => (
                                      <button
                                        key={stepKey}
                                        className={stepKey === runSelectedStepKey ? "active" : ""}
                                        onClick={() => handleRunStepSelect(stepKey)}
                                      >
                                        {stepKey}
                                      </button>
                                    ))}
                                    {steps.length > 8 ? <span className="muted">+{steps.length - 8} more</span> : null}
                                  </div>
                                ) : null}
                              </section>
                            );
                          })}
                        </aside>
                      </div>

                      <section className="run-events-pane">
                        <div className="panel-head compact">
                          <h4>Event Feed</h4>
                          <span className="muted">
                            {filteredRunEvents.length} / {runEvents.length} loaded
                            {runEventsTotal > runEvents.length ? ` (latest ${runEvents.length} of ${runEventsTotal})` : ` (${runEventsTotal} total)`}
                          </span>
                          {runEventsBusy ? <span className="muted">Updating…</span> : null}
                        </div>

                        <div className="run-events-controls">
                          <label>
                            Level
                            <select value={runEventLevelFilter} onChange={(event) => setRunEventLevelFilter(event.target.value)}>
                              <option value="all">All</option>
                              {runEventLevels.map((level) => (
                                <option key={level} value={level}>
                                  {level}
                                </option>
                              ))}
                            </select>
                          </label>

                          <label>
                            Step
                            <select value={runEventStepFilter} onChange={(event) => setRunEventStepFilter(event.target.value)}>
                              <option value="all">All steps</option>
                              {runDagLayout.nodes.map((node) => (
                                <option key={node.step_key} value={node.step_key}>
                                  {node.step_key}
                                </option>
                              ))}
                            </select>
                          </label>

                          <div className="run-stream-toggle">
                            {(["all", "stdout", "stderr"] as EventStreamFilter[]).map((stream) => (
                              <button
                                key={stream}
                                className={`ghost-btn tiny ${runEventStreamFilter === stream ? "active" : ""}`}
                                onClick={() => setRunEventStreamFilter(stream)}
                              >
                                {stream}
                              </button>
                            ))}
                          </div>

                          <label className="stretch">
                            Search
                            <input
                              value={runEventSearch}
                              onChange={(event) => setRunEventSearch(event.target.value)}
                              placeholder="event text, type, step"
                            />
                          </label>
                        </div>

                        <div className="run-events-table-wrap" ref={runEventsViewportRef}>
                          <table className="run-events-table">
                            <colgroup>
                              <col className="run-events-col-ts" />
                              <col className="run-events-col-step" />
                              <col className="run-events-col-type" />
                              <col className="run-events-col-message" />
                            </colgroup>
                            <thead>
                              <tr>
                                <th className="run-event-ts">Timestamp</th>
                                <th className="run-event-step">Step</th>
                                <th className="run-event-type">Event Type</th>
                                <th className="run-event-message">Message</th>
                              </tr>
                            </thead>
                            <tbody>
                              {filteredRunEvents.map((event) => {
                                const expandable = event.message.length > 200;
                                const collapsed = expandable && collapsedRunEventIDs.includes(event.id);
                                const message = collapsed ? truncateText(event.message, 200) : event.message;
                                const formattedEventData = collapsed ? "" : formatRunEventDataJSON(event.event_data_json);
                                const highlight = runHoveredStepKey.length > 0 && runHoveredStepKey === event.step_key;
                                const outcomeClass = runEventOutcomeClass(event);
                                const severityClass = isErrorEvent(event) ? "error" : isWarningEvent(event) ? "warning" : "";
                                return (
                                  <tr
                                    key={event.id}
                                    className={`run-event-row ${severityClass} ${outcomeClass} ${highlight ? "step-highlight" : ""}`}
                                  >
                                    <td className="run-event-ts">{formatTs(event.created_at)}</td>
                                    <td className="run-event-step">{event.step_key || "-"}</td>
                                    <td className="run-event-type">{event.event_type}</td>
                                    <td className="run-event-message-cell">
                                      <p>{message}</p>
                                      {expandable ? (
                                        <button className="ghost-btn tiny" onClick={() => toggleCollapsedRunEvent(event.id)}>
                                          {collapsed ? "Expand" : "Collapse"}
                                        </button>
                                      ) : null}
                                      {formattedEventData ? (
                                        <pre className="run-event-json">{formattedEventData}</pre>
                                      ) : null}
                                    </td>
                                  </tr>
                                );
                              })}
                              {filteredRunEvents.length === 0 ? (
                                <tr>
                                  <td className="runs-empty-row" colSpan={4}>
                                    No events match the current filters.
                                  </td>
                                </tr>
                              ) : null}
                            </tbody>
                          </table>
                        </div>
                      </section>
                    </>
                  )}
                </section>
              )}
            </div>
          ) : null}
        </main>

        {runHealthPopover ? (
          <button
            ref={runHealthPopoverRef}
            type="button"
            className={`run-health-popover ${runHealthPopover.placement}`}
            style={{ left: `${runHealthPopover.left}px`, top: `${runHealthPopover.top}px` }}
            onMouseEnter={() => {
              if (runHealthPopoverHideTimerRef.current !== null) {
                window.clearTimeout(runHealthPopoverHideTimerRef.current);
                runHealthPopoverHideTimerRef.current = null;
              }
            }}
            onMouseLeave={() => scheduleRunHealthPopoverHide()}
            onClick={() => {
              if (runHealthPopover.run?.run_key) {
                openRun(runHealthPopover.run.run_key);
              }
              hideRunHealthPopover();
            }}
            aria-label={
              runHealthPopover.run?.run_key
                ? `Open run ${runHealthPopover.run.run_key}`
                : "Run details unavailable"
            }
          >
            <div className="run-health-popover-job">{runHealthPopover.jobLabel}</div>
            <div className="run-health-popover-divider" />
            {runHealthPopover.run ? (
              <div className="run-health-popover-rows">
                <div className="run-health-popover-row">
                  <span
                    className={`run-health-popover-icon ${normalizeStatus(runHealthPopover.run.status)} ${
                      normalizeStatus(runHealthPopover.run.status) === "running" ? "spin" : ""
                    }`}
                    aria-hidden="true"
                  >
                    {runStatusIcon(normalizeStatus(runHealthPopover.run.status))}
                  </span>
                  <span className="run-health-popover-label">Run ID</span>
                  <code className="run-health-popover-value">{shortRunKey(runHealthPopover.run.run_key)}</code>
                </div>
                <div className="run-health-popover-row">
                  <span className="run-health-popover-icon spacer" aria-hidden="true">
                    •
                  </span>
                  <span className="run-health-popover-label">Timestamp</span>
                  <span className="run-health-popover-value">
                    {formatTs(runHealthPopover.run.started_at || runHealthPopover.run.queued_at || runHealthPopover.run.completed_at)}
                  </span>
                </div>
                <div className="run-health-popover-row">
                  <span className="run-health-popover-icon spacer" aria-hidden="true">
                    •
                  </span>
                  <span className="run-health-popover-label">Duration</span>
                  <span className="run-health-popover-value">{formatRunDurationForPopover(runHealthPopover.run, refreshClockMs)}</span>
                </div>
              </div>
            ) : (
              <p className="run-health-popover-empty">Run details unavailable</p>
            )}
          </button>
        ) : null}
      </div>
    </div>
  );
}

function sortRunsByFreshness(rows: RunSummary[]): RunSummary[] {
  return [...rows].sort((left, right) => runTimestampMs(right) - runTimestampMs(left) || right.id - left.id);
}

function statusRank(status: string): number {
  const rank = STATUS_ORDER.indexOf(normalizeStatus(status));
  return rank === -1 ? STATUS_ORDER.length : rank;
}

function normalizeStatus(status: string): string {
  const normalized = (status || "").trim().toLowerCase();
  if (normalized === "cancelled") {
    return "canceled";
  }
  if (normalized === "in_progress") {
    return "running";
  }
  if ((STATUS_ORDER as readonly string[]).includes(normalized)) {
    return normalized;
  }
  if (normalized === "idle") {
    return "pending";
  }
  return "pending";
}

function runTimestampMs(run: RunSummary): number {
  const candidates = [run.started_at, run.queued_at, run.completed_at];
  for (const candidate of candidates) {
    const ts = parseTimestamp(candidate);
    if (ts > 0) {
      return ts;
    }
  }
  return 0;
}

function parseTimestamp(value: string): number {
  if (!value) return 0;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return 0;
  }
  return date.getTime();
}

function formatRelativeTimeFromNow(value: string, nowMs: number): string {
  const ts = parseTimestamp(value);
  if (ts <= 0) {
    return "-";
  }
  const reference = nowMs > 0 ? nowMs : Date.now();
  const diffMs = reference - ts;
  const absMs = Math.abs(diffMs);
  const absMinutes = Math.floor(absMs / MINUTE_MS);
  const absHours = Math.floor(absMs / HOUR_MS);
  const absDays = Math.floor(absMs / DAY_MS);

  if (diffMs < 0) {
    if (absMinutes < 1) {
      return "in <1 min";
    }
    if (absMinutes < 60) {
      return `in ${absMinutes} min`;
    }
    if (absHours < 24) {
      return `in ${absHours} hr`;
    }
    return `in ${absDays} day${absDays === 1 ? "" : "s"}`;
  }

  if (absMinutes < 1) {
    return "just now";
  }
  if (absMinutes < 60) {
    return `${absMinutes} min ago`;
  }
  if (absHours < 24) {
    return `${absHours} hr ago`;
  }
  if (absDays === 1) {
    return "Yesterday";
  }
  if (absDays < 7) {
    return `${absDays} days ago`;
  }
  return new Date(ts).toLocaleDateString([], { month: "short", day: "numeric" });
}

function formatDurationMsLargest(ms: number): string {
  if (ms <= 0) {
    return "";
  }
  if (ms < 1000) {
    return `${Math.ceil(ms)}ms`;
  }

  const totalSeconds = Math.ceil(ms / 1000);
  if (totalSeconds < 60) {
    return `${totalSeconds}s`;
  }

  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }

  const totalMinutes = Math.floor(totalSeconds / 60);
  return `${totalMinutes}:${String(seconds).padStart(2, "0")}`;
}

function runDurationLabel(run: RunSummary): string {
  const start = parseTimestamp(run.started_at);
  const end = parseTimestamp(run.completed_at);
  if (start <= 0 || end <= 0 || end < start) {
    return "";
  }
  const seconds = Math.round((end - start) / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return `${minutes}m ${remainder}s`;
}

function runDurationMs(run: RunSummary): number {
  const start = parseTimestamp(run.started_at);
  const end = parseTimestamp(run.completed_at);
  if (start <= 0 || end <= 0 || end < start) {
    return 0;
  }
  return end - start;
}

function formatRunDurationCell(run: RunSummary): string {
  const label = runDurationLabel(run);
  if (label) {
    return label;
  }
  const status = normalizeStatus(run.status);
  if (status === "running") {
    return "Running";
  }
  if (status === "queued" || status === "pending") {
    return "Queued";
  }
  return "-";
}

function runStatusLabel(status: string): string {
  switch (normalizeStatus(status)) {
    case "success":
      return "Success";
    case "failed":
      return "Failed";
    case "running":
      return "Running";
    case "queued":
      return "Queued";
    case "pending":
      return "Scheduled";
    case "canceled":
      return "Canceled";
    case "skipped":
      return "Skipped";
    default:
      return "Pending";
  }
}

function runStatusIcon(status: string): string {
  const normalized = normalizeStatus(status);
  if (normalized === "success") return "✓";
  if (normalized === "failed" || normalized === "canceled") return "✗";
  if (normalized === "running") return "↻";
  if (normalized === "queued" || normalized === "pending") return "•";
  return "•";
}

function shortRunKey(runKey: string): string {
  const key = (runKey || "").trim();
  if (!key) {
    return "-";
  }
  return key.length <= 8 ? key : key.slice(0, 8);
}

function formatRunDurationForPopover(run: RunSummary, nowMs: number): string {
  const start = parseTimestamp(run.started_at || run.queued_at);
  if (start <= 0) {
    return "-";
  }
  const normalized = normalizeStatus(run.status);
  if (normalized === "running" || normalized === "queued") {
    return formatDurationMsLargest(Math.max(0, nowMs - start));
  }
  const end = parseTimestamp(run.completed_at);
  if (end > 0 && end >= start) {
    return formatDurationMsLargest(end - start);
  }
  return "-";
}

function matchesRunQuickFilter(run: RunSummary, filter: RunsQuickFilter): boolean {
  if (filter === "all") {
    return true;
  }
  const status = normalizeStatus(run.status);
  const triggeredBy = (run.triggered_by || "").toLowerCase();
  if (filter === "backfills") {
    return triggeredBy.includes("backfill");
  }
  if (filter === "queued") {
    return status === "queued" || status === "pending";
  }
  if (filter === "in_progress") {
    return status === "running";
  }
  if (filter === "failed") {
    return status === "failed";
  }
  if (filter === "scheduled") {
    return triggeredBy.includes("schedule");
  }
  return true;
}

function runStepGroupFromStatus(status: string): RunStepGroupKey {
  const normalized = normalizeStatus(status);
  if (normalized === "failed" || normalized === "canceled") {
    return "failed";
  }
  if (normalized === "running") {
    return "executing";
  }
  if (normalized === "success" || normalized === "skipped") {
    return "succeeded";
  }
  if (normalized === "queued") {
    return "preparing";
  }
  return "not_executed";
}

function runStepVisualState(status: string): "executed" | "succeeded" | "failed" | "not-executed" | "running" {
  const normalized = normalizeStatus(status);
  if (normalized === "failed" || normalized === "canceled") {
    return "failed";
  }
  if (normalized === "running") {
    return "running";
  }
  if (normalized === "success") {
    return "succeeded";
  }
  if (normalized === "skipped") {
    return "executed";
  }
  return "not-executed";
}

function normalizeEventLevel(level: string): string {
  return (level || "").trim().toLowerCase();
}

function eventMatchesStream(event: RunEvent, stream: EventStreamFilter): boolean {
  if (stream === "all") {
    return true;
  }
  const haystack = `${event.event_type} ${event.message}`.toLowerCase();
  if (stream === "stdout") {
    return haystack.includes("stdout");
  }
  return haystack.includes("stderr");
}

function isErrorEvent(event: RunEvent): boolean {
  const level = normalizeEventLevel(event.level);
  if (level === "error" || level === "fatal") {
    return true;
  }
  const eventType = (event.event_type || "").toLowerCase();
  return eventType.includes("fail") || eventType.includes("error");
}

function isWarningEvent(event: RunEvent): boolean {
  const level = normalizeEventLevel(event.level);
  if (level === "warn" || level === "warning") {
    return true;
  }
  const eventType = (event.event_type || "").toLowerCase();
  return eventType.includes("warn");
}

function runEventOutcomeClass(event: RunEvent): string {
  const eventType = (event.event_type || "").trim().toLowerCase();
  if (eventType === "step_succeeded") {
    return "step-succeeded";
  }
  if (eventType === "step_failed") {
    return "step-failed";
  }
  return "";
}

function truncateText(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }
  return `${value.slice(0, maxLength - 1)}…`;
}

function formatRunEventDataJSON(raw: string): string {
  const trimmed = (raw || "").trim();
  if (!trimmed || trimmed === "{}") {
    return "";
  }
  let formatted = trimmed;
  try {
    const parsed = JSON.parse(trimmed);
    formatted = JSON.stringify(parsed, null, 2);
  } catch {
    formatted = trimmed;
  }
  return formatted
    .replace(/\\r\\n/g, "\n")
    .replace(/\\n/g, "\n")
    .replace(/\\t/g, "\t");
}

function formatRefreshCountdown(milliseconds: number): string {
  const totalSeconds = Math.max(0, Math.ceil(milliseconds / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function formatTs(value: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function formatTsShort(ms: number): string {
  if (ms <= 0) return "-";
  const date = new Date(ms);
  return date.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatTimelineHour(ms: number): string {
  const date = new Date(ms);
  return date.toLocaleTimeString([], { hour: "numeric" }).replace(/\s/g, "");
}

function buildTimelineDateMarkers(
  startMs: number,
  endMs: number,
  windowMs: number,
): Array<{ key: string; left: string; label: string; start: boolean }> {
  if (startMs <= 0 || endMs <= startMs || windowMs <= 0) {
    return [];
  }
  const markers: Array<{ key: string; left: string; label: string; start: boolean }> = [
    {
      key: `start-${startMs}`,
      left: "8px",
      label: formatTimelineDateLabel(startMs),
      start: true,
    },
  ];
  let cursor = Math.floor(startMs / DAY_MS) * DAY_MS + DAY_MS;
  while (cursor <= endMs) {
    const leftPct = ((cursor - startMs) / windowMs) * 100;
    markers.push({
      key: `day-${cursor}`,
      left: `${leftPct}%`,
      label: formatTimelineDateLabel(cursor),
      start: false,
    });
    cursor += DAY_MS;
  }
  return markers;
}

function formatTimelineDateLabel(ms: number): string {
  return new Date(ms).toLocaleDateString([], {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
}

function formatCronHuman(cronExpr: string, timezone: string): string {
  const expression = cronExpr.trim();
  const tz = timezone.trim();
  if (!expression) {
    return tz ? `No schedule (${tz})` : "No schedule";
  }
  try {
    const human = cronstrue.toString(expression, { throwExceptionOnParseError: true });
    return tz ? `${human} (${tz})` : human;
  } catch {
    return tz ? `${expression} (${tz})` : expression;
  }
}

function formatCronRaw(cronExpr: string, timezone: string): string {
  const expression = cronExpr.trim();
  const tz = timezone.trim();
  if (!expression) {
    return "";
  }
  return tz ? `${expression} (${tz})` : expression;
}

function scheduleTextsEquivalent(left: string, right: string): boolean {
  const normalize = (value: string) => value.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  return normalize(left) === normalize(right);
}

export default App;
