/** Base URL for staff API. Empty = same-origin (`/api/...`), used with Vite dev proxy. */
export function resolveApiBase(): string {
  const v = import.meta.env.VITE_API_URL?.replace(/\/$/, "").trim();
  if (v) return v;
  if (import.meta.env.DEV) return "";
  return "";
}

/** Use Postgres-backed API instead of embedded applicant_profiles / seed pipeline. */
export function isLiveApiMode(): boolean {
  if (import.meta.env.VITE_API_URL?.trim()) return true;
  return import.meta.env.DEV;
}
