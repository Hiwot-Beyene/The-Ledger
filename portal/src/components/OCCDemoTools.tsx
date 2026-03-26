import { useCallback, useEffect, useState } from "react";
import { isLiveApiMode, resolveApiBase } from "@/lib/apiBase";

type Props = { applicationId?: string };

type ObservedRow = {
  event_id?: string;
  stream_position?: number;
  global_position?: number;
  event_type?: string;
  recorded_at?: string;
  causation_id?: string | null;
  correlation_id?: string | null;
};

export default function OCCDemoTools({ applicationId = "" }: Props) {
  const live = isLiveApiMode();
  const base = resolveApiBase();
  const [idDraft, setIdDraft] = useState(() => applicationId.trim());

  useEffect(() => {
    setIdDraft(applicationId.trim());
  }, [applicationId]);

  const app = idDraft.trim();

  const [busy, setBusy] = useState(false);
  const [proofRun, setProofRun] = useState<Record<string, unknown> | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [plan, setPlan] = useState<Record<string, unknown> | null>(null);
  const [planErr, setPlanErr] = useState<string | null>(null);
  const [planBusy, setPlanBusy] = useState(false);

  useEffect(() => {
    if (!live || !app) {
      setPlan(null);
      setPlanErr(null);
      setPlanBusy(false);
      return;
    }
    let cancel = false;
    setPlanBusy(true);
    setPlanErr(null);
    setPlan(null);
    fetch(`${base}/api/applications/${encodeURIComponent(app)}/ledger/append-concurrency-proof/plan`)
      .then(async (res) => {
        const body = (await res.json().catch(() => ({}))) as Record<string, unknown>;
        if (!res.ok) {
          const d = body.detail;
          throw new Error(typeof d === "string" ? d : `${res.status}: ${JSON.stringify(body).slice(0, 300)}`);
        }
        if (!cancel) setPlan(body);
      })
      .catch((e) => {
        if (cancel) return;
        setPlanErr(String(e));
      })
      .finally(() => {
        if (cancel) return;
        setPlanBusy(false);
      });
    return () => {
      cancel = true;
    };
  }, [base, live, app]);

  useEffect(() => {
    setProofRun(null);
    setErr(null);
  }, [app]);

  const run = useCallback(async () => {
    if (!live || !app) return;
    setBusy(true);
    setErr(null);
    setProofRun(null);
    try {
      const res = await fetch(`${base}/api/applications/${encodeURIComponent(app)}/ledger/append-concurrency-proof`, {
        method: "POST",
      });
      const body = (await res.json().catch(() => ({}))) as Record<string, unknown>;
      if (!res.ok) {
        setErr(
          `${res.status}: ${typeof (body as { detail?: string }).detail === "string" ? (body as { detail: string }).detail : JSON.stringify(body).slice(0, 400)}`,
        );
        return;
      }
      setProofRun(body);
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  }, [base, live, app]);

  if (!live) {
    return (
      <div className="rounded-2xl border border-white/[0.06] bg-apex-slate/10 p-6 text-sm text-apex-mist">
        Connect the staff API (dev proxy or <code className="text-apex-champagne">VITE_API_URL</code>) to run append concurrency proof.
      </div>
    );
  }

  const race = plan?.race_steps as Record<string, unknown> | undefined;
  const writers = (race?.writers as Array<Record<string, unknown>>) ?? [];
  const ev = typeof race?.expected_version === "number" ? race.expected_version : undefined;
  const streamId = typeof plan?.stream_id === "string" ? plan.stream_id : "";
  const proofEventType =
    typeof plan?.proof_event_type === "string"
      ? plan.proof_event_type
      : typeof plan?.ping_event_type === "string"
        ? plan.ping_event_type
        : "FraudScreeningRequested";
  const appendPath = typeof plan?.append_path === "string" ? plan.append_path : "ledger.event_store.EventStore.append";
  const mechanism = plan?.mechanism as Record<string, string> | undefined;
  const obs = plan?.postgres_observation as Record<string, unknown> | undefined;
  const disclaimer = typeof plan?.disclaimer === "string" ? plan.disclaimer : "";
  const planSnap =
    typeof plan?.plan_snapshot_at_utc === "string" ? plan.plan_snapshot_at_utc : null;

  const runMeta = proofRun?.proof_run as Record<string, unknown> | undefined;
  const tailRows = Array.isArray(proofRun?.stream_tail_observed)
    ? (proofRun!.stream_tail_observed as ObservedRow[])
    : [];
  const raceOut = Array.isArray(proofRun?.concurrent_results)
    ? (proofRun!.concurrent_results as Array<Record<string, unknown>>)
    : [];
  const retryBloc = proofRun?.retry as Record<string, unknown> | null | undefined;

  return (
    <div className="rounded-2xl border border-apex-gold/20 bg-apex-slate/15 p-6 md:p-8">
      <h2 className="font-display text-xl text-apex-ivory">Step 2 · Concurrency under pressure</h2>
      <div className="mt-4">
        <label htmlFor="occ-application-id" className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">
          Application ID
        </label>
        <input
          id="occ-application-id"
          type="text"
          value={idDraft}
          onChange={(e) => setIdDraft(e.target.value)}
          placeholder="e.g. APEX-0001"
          autoComplete="off"
          spellCheck={false}
          className="mt-2 w-full max-w-md rounded-lg border border-white/15 bg-black/30 px-3 py-2 font-mono text-sm text-apex-cream placeholder:text-apex-mist/50 focus:border-apex-gold/40 focus:outline-none focus:ring-1 focus:ring-apex-gold/30"
        />
        <p className="mt-2 text-[11px] text-apex-mist/90">
          Pre-filled from Navigator / pipeline when available; edit to race on any <span className="font-mono">loan-…</span> stream in Postgres.
        </p>
      </div>
      <p className="mt-3 font-mono text-[11px] text-apex-champagne/90">
        {app ? (
          <>
            application_id={app} · target {streamId || `loan-${app}`}
          </>
        ) : (
          <>Enter an application id to load stream version and run the proof.</>
        )}
      </p>
      <p className="mt-3 text-xs leading-relaxed text-apex-mist">
        Same code path as production: <span className="font-mono text-apex-champagne/90">{appendPath}</span> on your{" "}
        <span className="text-apex-cream">loan stream</span> in Postgres. The handler reads{" "}
        <span className="font-mono text-apex-champagne">event_streams.current_version</span>, then two coroutines append with that same{" "}
        <span className="font-mono">expected_version</span>. One CAS wins; the other gets{" "}
        <code className="text-apex-champagne/90">OptimisticConcurrencyError</code>, reloads the version, retries once (same pattern as agent OCC retries).
      </p>

      {mechanism ? (
        <div className="mt-4 rounded-xl border border-white/10 bg-black/25 p-4 text-xs text-apex-mist">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">Mechanism (server truth)</p>
          <ul className="mt-2 list-inside list-disc space-y-1">
            {mechanism.cas ? (
              <li>
                <span className="text-apex-mist">CAS: </span>
                <code className="text-[10px] text-apex-cream">{mechanism.cas}</code>
              </li>
            ) : null}
            {mechanism.why_conflict ? <li>{mechanism.why_conflict}</li> : null}
            {mechanism.retry_pattern ? <li>{mechanism.retry_pattern}</li> : null}
          </ul>
          {obs ? (
            <p className="mt-3 font-mono text-[10px] text-apex-cream">
              postgres: current_version={String(obs["event_streams.current_version"] ?? "—")} → expected_version for race=
              {String(obs["used_as_expected_version_for_concurrent_append"] ?? ev ?? "—")}
              {planSnap ? (
                <>
                  {" "}
                  · plan snapshot <span className="text-apex-mist/80">{planSnap}</span>
                </>
              ) : null}
            </p>
          ) : null}
        </div>
      ) : null}

      {disclaimer ? <p className="mt-3 text-[11px] text-apex-mist/90">{disclaimer}</p> : null}

      <div className="mt-4 rounded-xl border border-white/10 bg-apex-slate/20 p-4">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">What the two writers do</p>
        {planBusy ? (
          <p className="mt-3 text-sm text-apex-mist">Loading live stream version from database…</p>
        ) : planErr ? (
          <p className="mt-3 text-sm text-rose-100/90">{planErr}</p>
        ) : writers.length && ev !== undefined && streamId ? (
          <>
            <div className="mt-3 space-y-3">
              {writers.map((w) => {
                const name = String(w.agent ?? "");
                const append = w.append as Record<string, unknown> | undefined;
                const evType = String(append?.event_type ?? proofEventType);
                const caus = String(append?.causation_id ?? "");
                const corr = String(append?.correlation_id ?? "");
                const shape = append?.payload_shape != null ? String(append.payload_shape) : "";
                return (
                  <div key={name} className="rounded-lg border border-white/10 bg-black/20 p-3">
                    <p className="font-mono text-[11px] text-apex-gold">{name}</p>
                    <p className="mt-1 text-xs text-apex-mist">
                      Append{" "}
                      <code className="text-apex-champagne">{evType}</code> to{" "}
                      <code className="text-apex-champagne">{streamId}</code> with{" "}
                      <span className="font-mono">expected_version={ev}</span>
                      {shape ? (
                        <>
                          {" "}
                          <span className="text-apex-mist/80">({shape})</span>
                        </>
                      ) : null}
                      {caus ? (
                        <>
                          {" "}
                          · causation <span className="font-mono text-apex-mist/80">{caus}</span>
                        </>
                      ) : null}
                      {corr ? (
                        <>
                          {" "}
                          · correlation <span className="font-mono text-apex-mist/80">{corr}</span>
                        </>
                      ) : null}
                      .
                    </p>
                  </div>
                );
              })}
            </div>
            <p className="mt-3 text-xs text-apex-mist">
              One append wins (CAS matches <span className="font-mono">current_version</span>). The loser gets{" "}
              <code className="text-apex-champagne/90">OptimisticConcurrencyError</code> and then retries with the latest
              stream version.
            </p>
          </>
        ) : (
          <p className="mt-3 text-sm text-apex-mist">No plan loaded.</p>
        )}
      </div>

      <div className="mt-6 flex flex-wrap gap-3">
        <button
          type="button"
          disabled={!app || busy || planBusy || !!planErr || ev === undefined}
          onClick={() => void run()}
          className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-6 py-2 text-sm font-semibold text-apex-void shadow-lg shadow-apex-gold/15 disabled:opacity-40"
        >
          {busy ? "Running…" : "Run append concurrency proof"}
        </button>
      </div>

      {err ? (
        <p className="mt-4 rounded-lg border border-rose-500/30 bg-rose-950/20 p-4 text-sm text-rose-100/90">{err}</p>
      ) : null}

      {proofRun ? (
        <div className="mt-6 space-y-4 rounded-xl border border-apex-gold/25 bg-black/20 p-4">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">Proof outcome (live response)</p>
          {runMeta ? (
            <dl className="grid gap-1 text-[11px] text-apex-mist sm:grid-cols-2">
              <dt className="text-apex-mist/70">Started (UTC)</dt>
              <dd className="font-mono text-apex-cream">{String(runMeta.started_at_utc ?? "—")}</dd>
              <dt className="text-apex-mist/70">Completed (UTC)</dt>
              <dd className="font-mono text-apex-cream">{String(runMeta.completed_at_utc ?? "—")}</dd>
              <dt className="text-apex-mist/70">Duration</dt>
              <dd className="font-mono text-apex-cream">{String(runMeta.duration_ms ?? "—")} ms</dd>
              <dt className="text-apex-mist/70">Observation source</dt>
              <dd className="font-mono text-apex-cream">{String(runMeta.observation_source ?? "—")}</dd>
            </dl>
          ) : null}
          <div className="text-xs text-apex-mist">
            <span className="text-apex-mist/70">Pre-race expected_version</span>{" "}
            <span className="font-mono text-apex-cream">
              {String(proofRun.pre_race_stream_version_used_as_expected_version ?? "—")}
            </span>
            {" · "}
            <span className="text-apex-mist/70">final stream version</span>{" "}
            <span className="font-mono text-apex-cream">{String(proofRun.final_stream_version ?? "—")}</span>
          </div>
          {raceOut.length ? (
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">Concurrent writers</p>
              <ul className="mt-2 space-y-2">
                {raceOut.map((r) => (
                  <li
                    key={String(r.agent)}
                    className="rounded-lg border border-white/10 bg-apex-slate/25 px-3 py-2 font-mono text-[11px] text-apex-cream"
                  >
                    <span className="text-apex-gold">{String(r.agent)}</span> · {String(r.outcome)}
                    {typeof r.new_version === "number" ? <> → version {r.new_version}</> : null}
                    {r.diagnostic ? (
                      <pre className="mt-2 max-h-40 overflow-auto text-[10px] text-apex-mist">
                        {JSON.stringify(r.diagnostic, null, 2)}
                      </pre>
                    ) : null}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
          {retryBloc && Object.keys(retryBloc).length ? (
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">Retry</p>
              <pre className="mt-2 max-h-48 overflow-auto rounded-lg border border-white/10 bg-black/30 p-2 text-[10px] text-apex-mist">
                {JSON.stringify(retryBloc, null, 2)}
              </pre>
            </div>
          ) : null}
          {tailRows.length ? (
            <div>
              <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">
                Stream tail from Postgres after run ({tailRows.length} events)
              </p>
              <div className="mt-2 overflow-x-auto">
                <table className="w-full min-w-[640px] border-collapse text-left text-[10px] text-apex-mist">
                  <thead>
                    <tr className="border-b border-white/10 text-apex-champagne/90">
                      <th className="py-2 pr-2 font-semibold">pos</th>
                      <th className="py-2 pr-2 font-semibold">type</th>
                      <th className="py-2 pr-2 font-semibold">event_id</th>
                      <th className="py-2 pr-2 font-semibold">recorded_at</th>
                      <th className="py-2 pr-2 font-semibold">causation</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tailRows.map((row) => (
                      <tr key={`${row.stream_position}-${row.event_id}`} className="border-b border-white/[0.06]">
                        <td className="py-1.5 pr-2 font-mono text-apex-cream">{row.stream_position ?? "—"}</td>
                        <td className="py-1.5 pr-2 font-mono">{row.event_type ?? "—"}</td>
                        <td className="max-w-[140px] truncate py-1.5 pr-2 font-mono" title={row.event_id}>
                          {row.event_id ?? "—"}
                        </td>
                        <td className="py-1.5 pr-2 font-mono text-[9px]">{row.recorded_at ?? "—"}</td>
                        <td className="max-w-[180px] truncate py-1.5 font-mono text-[9px]" title={String(row.causation_id ?? "")}>
                          {row.causation_id ?? "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : null}
          <details className="text-xs text-apex-mist">
            <summary className="cursor-pointer text-apex-champagne/90">Raw JSON (full payload)</summary>
            <pre className="mt-2 max-h-64 overflow-auto rounded-lg border border-white/10 bg-black/40 p-3 text-[10px] text-apex-cream">
              {JSON.stringify(proofRun, null, 2)}
            </pre>
          </details>
        </div>
      ) : null}
    </div>
  );
}
