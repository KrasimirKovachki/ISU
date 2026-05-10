/**
 * Tiny typed API client for the FastAPI backend.
 *
 * - Server components call `apiFetch` directly (it works in any runtime).
 * - Client components use TanStack Query hooks built on top of these
 *   functions; see `hooks/use-queries.ts` for the React side.
 *
 * Base URL resolution:
 *   - server context  -> SKATING_API_URL (defaults to http://127.0.0.1:8000)
 *   - browser context -> NEXT_PUBLIC_SKATING_API_URL (defaults to /api proxy)
 *
 * The default browser path is `""` (relative) which lets a Next.js rewrite
 * proxy /api/* to the FastAPI backend, avoiding CORS in production.
 */
import type {
  DashboardResponse,
  SkaterSearchRow,
  SkaterDetail,
  EventRow,
  EventDetail,
  ScoreDetail,
  NationalityRow,
  ClubRow,
} from "./types";

const SERVER_BASE = process.env.SKATING_API_URL || "http://127.0.0.1:8000";
const BROWSER_BASE = process.env.NEXT_PUBLIC_SKATING_API_URL || "";

function baseUrl() {
  return typeof window === "undefined" ? SERVER_BASE : BROWSER_BASE;
}

export async function apiFetch<T>(
  path: string,
  init?: RequestInit & { revalidate?: number }
): Promise<T> {
  const url = `${baseUrl()}${path}`;
  const res = await fetch(url, {
    ...init,
    next: init?.revalidate !== undefined ? { revalidate: init.revalidate } : undefined,
    headers: { Accept: "application/json", ...(init?.headers || {}) },
  });
  if (!res.ok) {
    throw new Error(`API ${res.status} on ${path}: ${await res.text().catch(() => "")}`);
  }
  return (await res.json()) as T;
}

// --- single-purpose typed wrappers ---------------------------------------

export function getDashboard(period?: { from?: number | null; to?: number | null }) {
  const qs = new URLSearchParams();
  if (period?.from) qs.set("from_year", String(period.from));
  if (period?.to) qs.set("to_year", String(period.to));
  const q = qs.toString();
  return apiFetch<DashboardResponse>(`/api/dashboard${q ? `?${q}` : ""}`, { revalidate: 30 });
}

export function searchSkaters(q?: string, limit = 100) {
  const qs = new URLSearchParams({ limit: String(limit) });
  if (q) qs.set("q", q);
  return apiFetch<{ items: SkaterSearchRow[] }>(`/api/skaters?${qs}`);
}

export function getSkater(skaterKey: number) {
  return apiFetch<SkaterDetail>(`/api/skaters/${skaterKey}`, { revalidate: 60 });
}

export function listEvents(opts?: { q?: string; from_year?: number; to_year?: number; limit?: number }) {
  const qs = new URLSearchParams({ limit: String(opts?.limit ?? 200) });
  if (opts?.q) qs.set("q", opts.q);
  if (opts?.from_year) qs.set("from_year", String(opts.from_year));
  if (opts?.to_year) qs.set("to_year", String(opts.to_year));
  return apiFetch<{ items: EventRow[] }>(`/api/events?${qs}`);
}

export function getEvent(eventKey: number) {
  return apiFetch<EventDetail>(`/api/events/${eventKey}`, { revalidate: 60 });
}

export function getScore(segmentScoreKey: number) {
  return apiFetch<ScoreDetail>(`/api/scores/${segmentScoreKey}`, { revalidate: 60 });
}

export function listClubs(q?: string) {
  const qs = new URLSearchParams({ limit: "300" });
  if (q) qs.set("q", q);
  return apiFetch<{ items: ClubRow[] }>(`/api/clubs?${qs}`);
}

export function listNationalities(q?: string) {
  const qs = new URLSearchParams({ limit: "200" });
  if (q) qs.set("q", q);
  return apiFetch<{ items: NationalityRow[] }>(`/api/nationalities?${qs}`);
}
