export const FUTURE_WINDOW_MIN = 0;
export const FUTURE_WINDOW_MAX = 365;
export const TTL_MINUTES_MIN = 1;
export const TTL_MINUTES_MAX = 10080;

export function clampFutureWindowDays(value: unknown): number {
    const n = typeof value === 'number' ? value : parseInt(String(value), 10);
    if (!Number.isFinite(n)) return 30;
    return Math.min(Math.max(Math.trunc(n), FUTURE_WINDOW_MIN), FUTURE_WINDOW_MAX);
}

export function clampTtlMinutes(value: unknown): number {
    const n = typeof value === 'number' ? value : parseInt(String(value), 10);
    if (!Number.isFinite(n)) return 15;
    return Math.min(Math.max(Math.trunc(n), TTL_MINUTES_MIN), TTL_MINUTES_MAX);
}
