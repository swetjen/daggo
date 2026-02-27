import { describe, expect, test } from "bun:test";
import { listUpcomingScheduleTimestamps } from "./timelineSchedule";

describe("listUpcomingScheduleTimestamps", () => {
  test("expands a 5-minute cron across the visible future window", () => {
    const nowMs = Date.parse("2026-02-23T18:00:00.000Z");
    const endMs = Date.parse("2026-02-23T18:15:00.000Z");

    const occurrences = listUpcomingScheduleTimestamps(
      {
        cronExpr: "*/5 * * * *",
        timezone: "UTC",
      },
      nowMs,
      endMs,
    );

    expect(occurrences.map((timestamp) => new Date(timestamp).toISOString())).toEqual([
      "2026-02-23T18:05:00.000Z",
      "2026-02-23T18:10:00.000Z",
      "2026-02-23T18:15:00.000Z",
    ]);
  });

  test("caps expanded occurrences for high-frequency schedules", () => {
    const nowMs = Date.parse("2026-02-23T18:00:00.000Z");
    const endMs = Date.parse("2026-02-23T19:00:00.000Z");

    const occurrences = listUpcomingScheduleTimestamps(
      {
        cronExpr: "* * * * *",
        timezone: "UTC",
      },
      nowMs,
      endMs,
      { maxOccurrences: 5 },
    );

    expect(occurrences).toHaveLength(5);
    expect(new Date(occurrences[0]).toISOString()).toBe("2026-02-23T18:01:00.000Z");
    expect(new Date(occurrences[4]).toISOString()).toBe("2026-02-23T18:05:00.000Z");
  });
});
