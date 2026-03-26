/** Display timestamps from ISO-8601 API values in the user's locale. */
export function formatDateTime(iso: string | null | undefined): string {
  if (iso == null || String(iso).trim() === "") return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  try {
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "short",
    }).format(d);
  } catch {
    try {
      return d.toLocaleString();
    } catch {
      return d.toISOString();
    }
  }
}

/**
 * Readable label + canonical ISO. Avoids exotic Intl option mixes — some engines throw
 * `TypeError: Invalid option` (e.g. mixing weekday with dateStyle, or strict locale tables).
 */
export function formatInstantForUi(iso: string | null | undefined): { readable: string; iso: string | null } {
  if (iso == null || String(iso).trim() === "") return { readable: "—", iso: null };
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return { readable: String(iso), iso: String(iso) };
  let readable: string;
  try {
    readable = formatDateTime(d.toISOString());
  } catch {
    readable = d.toISOString();
  }
  if (readable === "—") readable = d.toISOString();
  let isoOut: string;
  try {
    isoOut = d.toISOString();
  } catch {
    isoOut = String(iso);
  }
  return { readable, iso: isoOut };
}

export function formatConfidence(raw: unknown): string {
  if (raw == null) return "—";
  if (typeof raw === "number" && !Number.isNaN(raw)) {
    if (raw > 0 && raw <= 1) {
      const pct = Math.round(raw * 1000) / 10;
      return `${pct}%`;
    }
    return String(raw);
  }
  return String(raw);
}
