const MINUTE_MS = 60 * 1000;
const DEFAULT_MAX_OCCURRENCES = 720;

type ParsedCronField = {
  any: boolean;
  values: Set<number>;
};

type ParsedCronSpec = {
  minute: ParsedCronField;
  hour: ParsedCronField;
  dom: ParsedCronField;
  month: ParsedCronField;
  dow: ParsedCronField;
};

type CronFieldKind = "minute" | "hour" | "dom" | "month" | "dow";

type CronTimeParts = {
  minute: number;
  hour: number;
  dom: number;
  month: number;
  dow: number;
};

export type TimelineSchedule = {
  cronExpr: string;
  timezone: string;
};

export type TimelineScheduleExpansionOptions = {
  maxOccurrences?: number;
};

const CRON_WEEKDAY_INDEX: Record<string, number> = {
  Sun: 0,
  Mon: 1,
  Tue: 2,
  Wed: 3,
  Thu: 4,
  Fri: 5,
  Sat: 6,
};

const CRON_DOW_NAME_TO_NUMBER: Record<string, number> = {
  sun: 0,
  mon: 1,
  tue: 2,
  wed: 3,
  thu: 4,
  fri: 5,
  sat: 6,
};

const CRON_MONTH_NAME_TO_NUMBER: Record<string, number> = {
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  may: 5,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  oct: 10,
  nov: 11,
  dec: 12,
};

const cronTimeFormatterByTZ = new Map<string, Intl.DateTimeFormat>();
const parsedCronSpecCache = new Map<string, ParsedCronSpec | null>();

export function listUpcomingScheduleTimestamps(
  schedule: TimelineSchedule,
  startMs: number,
  endMs: number,
  options: TimelineScheduleExpansionOptions = {},
): number[] {
  if (endMs <= startMs) {
    return [];
  }

  const spec = parseCronSpecCached(schedule.cronExpr);
  if (!spec) {
    return [];
  }

  const timezone = normalizeScheduleTimezone(schedule.timezone);
  const maxOccurrences = normalizeMaxOccurrences(options.maxOccurrences);
  if (maxOccurrences === 0) {
    return [];
  }

  const firstCandidateMs = alignToNextMinute(startMs + 1);
  const ceilingMs = Math.floor(endMs / MINUTE_MS) * MINUTE_MS;
  if (firstCandidateMs > ceilingMs) {
    return [];
  }

  const timestamps: number[] = [];
  for (let cursorMs = firstCandidateMs; cursorMs <= ceilingMs; cursorMs += MINUTE_MS) {
    if (timestamps.length >= maxOccurrences) {
      break;
    }
    const parts = extractCronTimeParts(cursorMs, timezone);
    if (!parts) {
      continue;
    }
    if (cronSpecMatches(spec, parts)) {
      timestamps.push(cursorMs);
    }
  }

  return timestamps;
}

function normalizeMaxOccurrences(maxOccurrences: number | undefined): number {
  if (!Number.isFinite(maxOccurrences)) {
    return DEFAULT_MAX_OCCURRENCES;
  }
  const normalized = Math.max(0, Math.floor(maxOccurrences ?? DEFAULT_MAX_OCCURRENCES));
  return normalized;
}

function alignToNextMinute(timestampMs: number): number {
  return Math.ceil(timestampMs / MINUTE_MS) * MINUTE_MS;
}

function parseCronSpecCached(cronExpr: string): ParsedCronSpec | null {
  const key = cronExpr.trim();
  if (!key) {
    return null;
  }
  if (parsedCronSpecCache.has(key)) {
    return parsedCronSpecCache.get(key) ?? null;
  }
  const parsed = parseCronSpec(key);
  parsedCronSpecCache.set(key, parsed);
  return parsed;
}

function parseCronSpec(cronExpr: string): ParsedCronSpec | null {
  const segments = cronExpr.trim().split(/\s+/).filter(Boolean);
  if (segments.length !== 5) {
    return null;
  }

  const minute = parseCronField(segments[0], 0, 59, "minute");
  const hour = parseCronField(segments[1], 0, 23, "hour");
  const dom = parseCronField(segments[2], 1, 31, "dom");
  const month = parseCronField(segments[3], 1, 12, "month");
  const dow = parseCronField(segments[4], 0, 6, "dow");

  if (!minute || !hour || !dom || !month || !dow) {
    return null;
  }

  return { minute, hour, dom, month, dow };
}

function parseCronField(field: string, min: number, max: number, kind: CronFieldKind): ParsedCronField | null {
  const tokens = field
    .trim()
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean);

  if (tokens.length === 0) {
    return null;
  }

  const any = tokens.length === 1 && tokens[0] === "*";
  const values = new Set<number>();

  for (const token of tokens) {
    let base = token;
    let step = 1;
    const stepParts = token.split("/");

    if (stepParts.length > 2) {
      return null;
    }

    if (stepParts.length === 2) {
      base = stepParts[0].trim();
      step = Number(stepParts[1]);
      if (!Number.isInteger(step) || step <= 0) {
        return null;
      }
    }

    let start = min;
    let end = max;
    if (base !== "*") {
      const rangeParts = base.split("-");
      if (rangeParts.length > 2) {
        return null;
      }

      if (rangeParts.length === 2) {
        const parsedStart = parseCronTokenValue(rangeParts[0].trim(), kind);
        const parsedEnd = parseCronTokenValue(rangeParts[1].trim(), kind);
        if (parsedStart === null || parsedEnd === null) {
          return null;
        }
        start = parsedStart;
        end = parsedEnd;
      } else {
        const parsedValue = parseCronTokenValue(base.trim(), kind);
        if (parsedValue === null) {
          return null;
        }
        start = parsedValue;
        end = parsedValue;
      }
    }

    if (start < min || end > max || start > end) {
      return null;
    }

    for (let value = start; value <= end; value += step) {
      values.add(kind === "dow" && value === 7 ? 0 : value);
    }
  }

  return { any, values };
}

function parseCronTokenValue(raw: string, kind: CronFieldKind): number | null {
  const value = raw.trim().toLowerCase();
  if (!value) {
    return null;
  }

  if (kind === "month" && value in CRON_MONTH_NAME_TO_NUMBER) {
    return CRON_MONTH_NAME_TO_NUMBER[value];
  }
  if (kind === "dow" && value in CRON_DOW_NAME_TO_NUMBER) {
    return CRON_DOW_NAME_TO_NUMBER[value];
  }

  const numeric = Number(value);
  if (!Number.isInteger(numeric)) {
    return null;
  }

  if (kind === "dow" && numeric === 7) {
    return 0;
  }

  return numeric;
}

function cronSpecMatches(spec: ParsedCronSpec, parts: CronTimeParts): boolean {
  if (!spec.minute.values.has(parts.minute)) {
    return false;
  }
  if (!spec.hour.values.has(parts.hour)) {
    return false;
  }
  if (!spec.month.values.has(parts.month)) {
    return false;
  }

  const domMatch = spec.dom.values.has(parts.dom);
  const dowMatch = spec.dow.values.has(parts.dow);

  if (spec.dom.any && spec.dow.any) {
    return true;
  }
  if (spec.dom.any) {
    return dowMatch;
  }
  if (spec.dow.any) {
    return domMatch;
  }

  return domMatch || dowMatch;
}

function extractCronTimeParts(timestampMs: number, timezone: string): CronTimeParts | null {
  const formatter = getCronTimeFormatter(timezone);
  const parts = formatter.formatToParts(new Date(timestampMs));
  let minute = -1;
  let hour = -1;
  let dom = -1;
  let month = -1;
  let weekday = "";

  for (const part of parts) {
    if (part.type === "minute") {
      minute = Number(part.value);
      continue;
    }
    if (part.type === "hour") {
      hour = Number(part.value);
      continue;
    }
    if (part.type === "day") {
      dom = Number(part.value);
      continue;
    }
    if (part.type === "month") {
      month = Number(part.value);
      continue;
    }
    if (part.type === "weekday") {
      weekday = part.value;
    }
  }

  if (!Number.isInteger(minute) || !Number.isInteger(hour) || !Number.isInteger(dom) || !Number.isInteger(month)) {
    return null;
  }

  if (hour === 24) {
    hour = 0;
  }

  const dow = CRON_WEEKDAY_INDEX[weekday];
  if (!Number.isInteger(dow)) {
    return null;
  }

  return { minute, hour, dom, month, dow };
}

function getCronTimeFormatter(timezone: string): Intl.DateTimeFormat {
  const normalized = normalizeScheduleTimezone(timezone);
  const cached = cronTimeFormatterByTZ.get(normalized);
  if (cached) {
    return cached;
  }

  try {
    const formatter = new Intl.DateTimeFormat("en-US", {
      timeZone: normalized,
      minute: "2-digit",
      hour: "2-digit",
      day: "2-digit",
      month: "2-digit",
      weekday: "short",
      hourCycle: "h23",
    });
    cronTimeFormatterByTZ.set(normalized, formatter);
    return formatter;
  } catch {
    if (normalized !== "UTC") {
      return getCronTimeFormatter("UTC");
    }
    const fallback = new Intl.DateTimeFormat("en-US", {
      minute: "2-digit",
      hour: "2-digit",
      day: "2-digit",
      month: "2-digit",
      weekday: "short",
      hourCycle: "h23",
    });
    cronTimeFormatterByTZ.set("UTC", fallback);
    return fallback;
  }
}

function normalizeScheduleTimezone(timezone: string): string {
  const trimmed = timezone.trim();
  return trimmed || "UTC";
}
