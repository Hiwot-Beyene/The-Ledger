import { useCallback, useEffect, useState } from "react";
import { isLiveApiMode, resolveApiBase } from "@/lib/apiBase";
import { formatDateTime } from "@/lib/formatDateTime";

type Props = { applicationId?: string };

type UpcastProofResponse = {
  ok?: boolean;
  stream_id?: string;
  stream_position?: number;
  database_row?: { event_type?: string; event_version?: number; payload?: Record<string, unknown> };
  through_event_store?: { event_type?: string; event_version?: number; payload?: Record<string, unknown> };
  stored_payload_bytes_unchanged_after_read?: boolean;
  stored_event_version_unchanged?: boolean;
  detail?: string;
};

export default function UpcastImmutabilityTools({ applicationId = "" }: Props) {
  const live = isLiveApiMode();
  const base = resolveApiBase();
  const [streamId, setStreamId] = useState("");
  const [position, setPosition] = useState("1");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [result, setResult] = useState<UpcastProofResponse | null>(null);
  const [fetchedAt, setFetchedAt] = useState<string | null>(null);

  useEffect(() => {
    const aid = applicationId.trim();
    setStreamId(aid ? `loan-${aid}` : "");
  }, [applicationId]);

  const run = useCallback(async () => {
    if (!live || !streamId.trim() || !position.trim()) return;
    const pos = Number(position);
    if (!Number.isInteger(pos) || pos < 0) {
      setErr("stream_position must be a non-negative integer.");
      return;
    }
    setBusy(true);
    setErr(null);
    setResult(null);
    setFetchedAt(null);
    try {
      const res = await fetch(
        `${base}/api/streams/${encodeURIComponent(streamId.trim())}/events/${pos}/upcast-proof`,
      );
      const body = (await res.json().catch(() => ({}))) as UpcastProofResponse;
      if (!res.ok) {
        setErr(typeof body.detail === "string" ? body.detail : `${res.status}: ${JSON.stringify(body).slice(0, 320)}`);
        return;
      }
      setResult(body);
      setFetchedAt(new Date().toISOString());
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  }, [base, live, streamId, position]);

  if (!live) {
    return (
      <div className="rounded-2xl border border-white/[0.06] bg-apex-slate/10 p-6 text-sm text-apex-mist">
        Connect the staff API (dev proxy or <code className="text-apex-champagne">VITE_API_URL</code>) for upcast proof.
      </div>
    );
  }

  const dbv = result?.database_row?.event_version;
  const stv = result?.through_event_store?.event_version;
  const upcasted = dbv !== undefined && stv !== undefined && dbv !== stv;
  const payloadOk = result?.stored_payload_bytes_unchanged_after_read === true;
  const verOk = result?.stored_event_version_unchanged === true;
  const et = result?.database_row?.event_type;
  const upcastEligibleType = et === "DecisionGenerated" || et === "CreditAnalysisCompleted";
  const explainNoUpcast =
    result?.ok && !upcasted && upcastEligibleType && dbv === stv && dbv === 2;

  return (
    <div className="rounded-2xl border border-apex-gold/20 bg-apex-slate/15 p-6 md:p-8">
      <h2 className="font-display text-xl text-apex-ivory">Step 4 · Upcasting &amp; immutability</h2>
      <p className="mt-2 max-w-3xl text-sm leading-relaxed text-apex-mist">
        <strong className="text-apex-cream">Demonstration:</strong> load one cell through{" "}
        <code className="text-apex-champagne/90">EventStore.load_stream</code> — if an upcaster applies, you see the{" "}
        <em>current</em> schema version in memory — then hit the same row in Postgres: the stored{" "}
        <code className="text-apex-champagne/90">payload</code> and <code className="text-apex-champagne/90">event_version</code>{" "}
        must stay exactly as written at append time (append-only immutability).
      </p>

      <div className="mt-4 rounded-xl border border-violet-500/25 bg-violet-950/15 p-4 text-xs leading-relaxed text-apex-mist">
        <p className="font-semibold text-violet-200/90">How to check this (hints)</p>
        <ol className="mt-2 list-inside list-decimal space-y-2">
          <li>
            In <strong className="text-apex-ivory">Step 1</strong>, run Navigator and note <code className="text-apex-champagne">stream_id</code> and{" "}
            <code className="text-apex-champagne">stream_position</code> from the event table in the package (or use{" "}
            <code className="text-apex-champagne">loan-{"{application_id}"}</code> and try positions <code className="text-apex-champagne">1</code>,{" "}
            <code className="text-apex-champagne">2</code>, …).
          </li>
          <li>
            Click <strong className="text-apex-ivory">Run upcast proof</strong> — the API runs a raw SQL read of <code className="text-apex-champagne">events</code>{" "}
            and compares to the same cell returned through the store (with upcasting).
          </li>
          <li>
            <strong className="text-apex-ivory">When versions match (common today):</strong> if the row was appended as{" "}
            <code className="text-apex-champagne">event_version: 2</code> for <code className="text-apex-champagne">DecisionGenerated</code> or{" "}
            <code className="text-apex-champagne">CreditAnalysisCompleted</code>, SQL and the store both show <strong>2</strong> — that is correct. Upcasters in this
            codebase only chain from <strong>v1 → v2</strong>; there is no upcaster for v2, so nothing changes on read.
          </li>
          <li>
            <strong className="text-apex-ivory">Success pattern for “show upcast”:</strong> <code className="text-apex-champagne">database_row.event_version</code>{" "}
            <strong>1</strong> while <code className="text-apex-champagne">through_event_store.event_version</code> is <strong>2</strong> (read-time only). Flags{" "}
            <code className="text-apex-champagne">stored_*_unchanged</code> stay <code className="text-apex-champagne">true</code> — the DB row is not updated by reading.
            Legacy rows or a dev-only SQL tweak can set disk to v1 to illustrate that path.
          </li>
          <li>
            Optional DB audit:{" "}
            <code className="break-all text-[10px] text-apex-champagne/80">
              SELECT event_id, event_version, payload FROM events WHERE stream_id = &apos;…&apos; AND stream_position = …;
            </code>{" "}
            before and after this call — values should be identical (append-only log).
          </li>
        </ol>
      </div>

      <div className="mt-6 flex flex-wrap items-end gap-3">
        <div className="min-w-[200px] flex-1">
          <label className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">stream_id</label>
          <input
            value={streamId}
            onChange={(e) => setStreamId(e.target.value)}
            placeholder="loan-APEX-0001"
            className="mt-1 w-full rounded-lg border border-white/15 bg-black/30 px-3 py-2 font-mono text-sm text-apex-cream"
          />
        </div>
        <div className="w-28">
          <label className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">position</label>
          <input
            value={position}
            onChange={(e) => setPosition(e.target.value)}
            placeholder="1"
            className="mt-1 w-full rounded-lg border border-white/15 bg-black/30 px-3 py-2 font-mono text-sm text-apex-cream"
          />
        </div>
        <button
          type="button"
          disabled={busy || !streamId.trim()}
          onClick={() => void run()}
          className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-6 py-2 text-sm font-semibold text-apex-void disabled:opacity-40"
        >
          {busy ? "Calling API…" : "Run upcast proof (live)"}
        </button>
      </div>
      <p className="mt-2 font-mono text-[10px] text-apex-mist/80">
        GET {base}/api/streams/<span className="text-apex-champagne">{streamId || "…"}</span>/events/
        <span className="text-apex-champagne">{position || "…"}</span>/upcast-proof
      </p>

      {err ? <p className="mt-4 rounded-lg border border-rose-500/30 bg-rose-950/20 p-3 text-sm text-rose-100/90">{err}</p> : null}

      {result?.ok ? (
        <div className="mt-6 space-y-4">
          {fetchedAt ? (
            <p className="text-[11px] text-apex-mist">
              Last response from server: <span className="font-mono text-apex-cream">{formatDateTime(fetchedAt)}</span> (
              {fetchedAt})
            </p>
          ) : null}

          {explainNoUpcast ? (
            <div className="rounded-xl border border-amber-500/30 bg-amber-950/15 p-4 text-xs leading-relaxed text-apex-mist">
              <p className="font-semibold text-amber-100/90">Same version — expected</p>
              <p className="mt-2">
                This event is already stored as <span className="font-mono text-apex-cream">event_version: 2</span>. The ledger only registers
                upcast steps from <span className="font-mono">(event_type, 1)</span> → 2, so the read path leaves it unchanged. You still proved{" "}
                <strong className="text-apex-ivory">immutability</strong>: raw SQL and store agree, and the row is not rewritten on read. To{" "}
                <em>show</em> a v1→v2 bump, the cell on disk would need to be version 1 (historical/import or local demo only).
              </p>
            </div>
          ) : null}

          <div className="grid gap-3 md:grid-cols-2">
            <div
              className={`rounded-xl border p-4 ${payloadOk && verOk ? "border-emerald-500/35 bg-emerald-950/10" : "border-white/10 bg-black/20"}`}
            >
              <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">Immutability flags</p>
              <ul className="mt-2 space-y-1 text-xs text-apex-mist">
                <li>
                  Payload unchanged after read:{" "}
                  <span className={payloadOk ? "font-mono text-emerald-200" : "font-mono text-rose-200"}>
                    {String(result.stored_payload_bytes_unchanged_after_read)}
                  </span>
                </li>
                <li>
                  Stored event_version unchanged:{" "}
                  <span className={verOk ? "font-mono text-emerald-200" : "font-mono text-rose-200"}>
                    {String(result.stored_event_version_unchanged)}
                  </span>
                </li>
                <li>
                  Upcast visible on read path:{" "}
                  <span className="font-mono text-apex-cream">
                    {upcasted ? `yes (DB v${dbv} → store v${stv})` : `no difference in reported versions (v${dbv ?? "?"})`}
                  </span>
                </li>
              </ul>
            </div>
            <div className="rounded-xl border border-white/10 bg-black/20 p-4">
              <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">Summary</p>
              <p className="mt-2 text-xs text-apex-mist">
                Raw row type <span className="font-mono text-apex-cream">{result.database_row?.event_type ?? "—"}</span> · Through
                store type <span className="font-mono text-apex-cream">{result.through_event_store?.event_type ?? "—"}</span>
              </p>
            </div>
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">database_row (SQL SELECT)</p>
              <pre className="mt-2 max-h-64 overflow-auto rounded-lg border border-white/10 bg-black/40 p-3 text-[10px] text-apex-cream">
                {JSON.stringify(result.database_row, null, 2)}
              </pre>
            </div>
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">through_event_store (upcasted read)</p>
              <pre className="mt-2 max-h-64 overflow-auto rounded-lg border border-white/10 bg-black/40 p-3 text-[10px] text-apex-cream">
                {JSON.stringify(result.through_event_store, null, 2)}
              </pre>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
