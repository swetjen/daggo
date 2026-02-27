import { describe, expect, test } from "bun:test";
import { buildTimelineArtifacts } from "./timelineArtifacts";

describe("buildTimelineArtifacts", () => {
  test("drops scheduled placeholders once their tick has passed", () => {
    const schedules = [{ schedule_key: "every_five", cron_expr: "*/5 * * * *", timezone: "UTC" }];
    const windowStartMs = Date.parse("2026-02-23T10:30:00.000Z");
    const windowEndMs = Date.parse("2026-02-23T10:45:00.000Z");

    const beforeTickPasses = buildTimelineArtifacts({
      jobKey: "daily_orders_pipeline",
      runs: [],
      schedules,
      nowMs: Date.parse("2026-02-23T10:34:00.000Z"),
      windowStartMs,
      windowEndMs,
    });

    expect(beforeTickPasses.filter((artifact) => artifact.state === "will_run").map((artifact) => artifact.timestampMs)).toEqual([
      Date.parse("2026-02-23T10:35:00.000Z"),
      Date.parse("2026-02-23T10:40:00.000Z"),
      Date.parse("2026-02-23T10:45:00.000Z"),
    ]);

    const afterTickPasses = buildTimelineArtifacts({
      jobKey: "daily_orders_pipeline",
      runs: [],
      schedules,
      nowMs: Date.parse("2026-02-23T10:36:00.000Z"),
      windowStartMs,
      windowEndMs,
    });

    expect(afterTickPasses.filter((artifact) => artifact.state === "will_run").map((artifact) => artifact.timestampMs)).toEqual([
      Date.parse("2026-02-23T10:40:00.000Z"),
      Date.parse("2026-02-23T10:45:00.000Z"),
    ]);
  });

  test("replaces a scheduled tick with a running marker when a scheduler run exists", () => {
    const artifacts = buildTimelineArtifacts({
      jobKey: "daily_orders_pipeline",
      runs: [
        {
          id: 22,
          run_key: "run_22",
          job_key: "daily_orders_pipeline",
          status: "running",
          triggered_by: "scheduler:every_five_minutes",
          queued_at: "2026-02-23T10:40:02.000Z",
          started_at: "2026-02-23T10:40:26.000Z",
          completed_at: "",
        },
      ],
      schedules: [{ schedule_key: "every_five", cron_expr: "*/5 * * * *", timezone: "UTC" }],
      nowMs: Date.parse("2026-02-23T10:39:00.000Z"),
      windowStartMs: Date.parse("2026-02-23T10:30:00.000Z"),
      windowEndMs: Date.parse("2026-02-23T10:50:00.000Z"),
    });

    const tenFortyTick = Date.parse("2026-02-23T10:40:00.000Z");
    const runningArtifact = artifacts.find((artifact) => artifact.runKey === "run_22");

    expect(runningArtifact?.state).toBe("running");
    expect(runningArtifact?.timestampMs).toBe(tenFortyTick);

    const duplicateScheduled = artifacts.find((artifact) => artifact.state === "will_run" && artifact.timestampMs === tenFortyTick);
    expect(duplicateScheduled).toBeUndefined();
  });

  test("maps completed scheduler runs to terminal states at the same tick", () => {
    const artifacts = buildTimelineArtifacts({
      jobKey: "daily_orders_pipeline",
      runs: [
        {
          id: 25,
          run_key: "run_25",
          job_key: "daily_orders_pipeline",
          status: "success",
          triggered_by: "scheduler:every_five_minutes",
          queued_at: "2026-02-23T10:45:00.000Z",
          started_at: "2026-02-23T10:45:22.000Z",
          completed_at: "2026-02-23T10:45:55.000Z",
        },
      ],
      schedules: [{ schedule_key: "every_five", cron_expr: "*/5 * * * *", timezone: "UTC" }],
      nowMs: Date.parse("2026-02-23T10:44:00.000Z"),
      windowStartMs: Date.parse("2026-02-23T10:30:00.000Z"),
      windowEndMs: Date.parse("2026-02-23T10:50:00.000Z"),
    });

    const runArtifact = artifacts.find((artifact) => artifact.runKey === "run_25");
    expect(runArtifact?.state).toBe("succeeded");
    expect(runArtifact?.timestampMs).toBe(Date.parse("2026-02-23T10:45:00.000Z"));
  });
});
