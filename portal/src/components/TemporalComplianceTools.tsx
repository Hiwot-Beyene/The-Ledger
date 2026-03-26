import { useCallback, useEffect, useState } from "react";
import { isLiveApiMode, resolveApiBase } from "@/lib/apiBase";
import { formatDateTime, formatInstantForUi } from "@/lib/formatDateTime";

type Props = { applicationId?: string };

type ComplianceResponse = {
  ok?: boolean;
  source?: string;
  as_of?: string;
  data?: Record<string, unknown>;
  detail?: string;
};

type DemoHints = {
  ok?: boolean;
  week5_prerequisite_step_3?: string;
  application_id?: string;
  ledger_resource_template?: string;
  http_equivalent?: string;
  compliance_snapshot_table_available?: boolean;
  temporal_backend_behavior?: string;
  first_compliance_check_completed_recorded_at_utc?: string;
  suggested_as_of_before_first_completion_utc?: string;
  suggested_as_of_after_first_completion_utc?: string;
  detail?: string;
};

function utcIsoToDatetimeLocalValue(iso: string): string | null {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function verdictSummary(data: Record<string, unknown> | undefined): string {
  if (!data) return "—";
  const v = data.overall_verdict;
  if (v == null) return "— (no verdict in projection)";
  if (typeof v === "object" && v !== null && "value" in v) return String((v as { value: unknown }).value);
  return String(v);
}

function summarizeComplianceData(data: Record<string, unknown> | undefined): { label: string; value: string }[] {
  if (!data) return [];
  const rows: { label: string; value: string }[] = [];
  const pick = (k: string, label?: string) => {
    const v = data[k];
    if (v == null || v === "") return;
    rows.push({ label: label ?? k, value: typeof v === "object" ? JSON.stringify(v) : String(v) });
  };
  pick("overall_verdict", "Overall verdict");
  pick("has_hard_block", "Hard block");
  pick("rules_evaluated", "Rules evaluated");
  pick("rules_passed", "Passed");
  pick("rules_failed", "Failed");
  pick("rules_noted", "Noted");
  const u = data["updated_at"];
  if (u != null && String(u).trim() !== "") {
    const s = typeof u === "string" ? u : JSON.stringify(u);
    rows.push({
      label: "Projection updated (replay)",
      value: Number.isNaN(new Date(s).getTime()) ? s : `${formatDateTime(s)}`,
    });
  }
  pick("application_id", "Application id");
  return rows;
}

function OutcomePanel({
  title,
  subtitle,
  body,
  err,
  busy,
}: {
  title: string;
  subtitle?: string;
  body: ComplianceResponse | null;
  err: string | null;
  busy: boolean;
}) {
  if (busy) {
    return (
      <div className="rounded-xl border border-white/10 bg-apex-slate/15 p-4 text-sm text-apex-mist">
        Loading…
      </div>
    );
  }
  if (err) {
    return (
      <div className="rounded-xl border border-rose-500/30 bg-rose-950/15 p-4 text-sm text-rose-100/90">{err}</div>
    );
  }
  if (!body?.ok) {
    return (
      <div className="rounded-xl border border-white/10 bg-apex-slate/15 p-4 text-sm text-apex-mist">
        No result yet.
      </div>
    );
  }
  const data = body.data as Record<string, unknown> | undefined;
  const summary = summarizeComplianceData(data);

  return (
    <div className="rounded-xl border border-white/10 bg-apex-slate/15 p-4">
      <p className="font-display text-sm font-semibold text-apex-ivory">{title}</p>
      {subtitle ? <p className="mt-1 text-[11px] text-apex-mist">{subtitle}</p> : null}
      <p className="mt-2 text-[11px] text-apex-champagne">
        source=<span className="text-apex-cream">{body.source ?? "—"}</span>
        {body.as_of ? (
          <>
            {" "}
            · point in time:{" "}
            <span className="font-medium text-apex-cream">{formatInstantForUi(body.as_of).readable}</span>
          </>
        ) : null}
      </p>
      {body.as_of ? (
        <p className="mt-1 font-mono text-[9px] text-apex-mist/70">ISO (API): {formatInstantForUi(body.as_of).iso}</p>
      ) : null}
      {summary.length ? (
        <dl className="mt-3 grid gap-1 text-[11px] sm:grid-cols-2">
          {summary.map((r) => (
            <div key={r.label} className="contents">
              <dt className="text-apex-mist/80">{r.label}</dt>
              <dd className="font-mono text-apex-cream">{r.value}</dd>
            </div>
          ))}
        </dl>
      ) : (
        <p className="mt-3 text-[11px] text-apex-mist">No summary fields; see raw JSON below.</p>
      )}
      <details className="mt-3 text-[11px] text-apex-mist">
        <summary className="cursor-pointer text-apex-champagne/90">Raw payload</summary>
        <pre className="mt-2 max-h-48 overflow-auto rounded-lg border border-white/10 bg-black/35 p-2 text-[10px] text-apex-cream">
          {JSON.stringify(body, null, 2)}
        </pre>
      </details>
    </div>
  );
}

export default function TemporalComplianceTools({ applicationId = "" }: Props) {
  const live = isLiveApiMode();
  const base = resolveApiBase();
  const [idDraft, setIdDraft] = useState(() => applicationId.trim());

  useEffect(() => {
    setIdDraft(applicationId.trim());
  }, [applicationId]);

  const app = idDraft.trim();

  const [asOfLocal, setAsOfLocal] = useState("");
  const [temporal, setTemporal] = useState<ComplianceResponse | null>(null);
  const [current, setCurrent] = useState<ComplianceResponse | null>(null);
  const [errTemporal, setErrTemporal] = useState<string | null>(null);
  const [errCurrent, setErrCurrent] = useState<string | null>(null);
  const [busyTemporal, setBusyTemporal] = useState(false);
  const [busyCurrent, setBusyCurrent] = useState(false);

  const [hints, setHints] = useState<DemoHints | null>(null);
  const [hintsErr, setHintsErr] = useState<string | null>(null);
  const [hintsBusy, setHintsBusy] = useState(false);

  useEffect(() => {
    setTemporal(null);
    setCurrent(null);
    setErrTemporal(null);
    setErrCurrent(null);
    setHints(null);
    setHintsErr(null);
  }, [app]);

  useEffect(() => {
    if (!live || !app) {
      setHints(null);
      setHintsErr(null);
      setHintsBusy(false);
      return;
    }
    let cancel = false;
    setHintsBusy(true);
    setHintsErr(null);
    setHints(null);
    fetch(`${base}/api/applications/${encodeURIComponent(app)}/compliance/temporal-demo-hints`)
      .then(async (res) => {
        const body = (await res.json().catch(() => ({}))) as DemoHints;
        if (!res.ok) {
          const d = body.detail;
          throw new Error(typeof d === "string" ? d : `${res.status}: ${JSON.stringify(body).slice(0, 320)}`);
        }
        if (!cancel) setHints(body);
      })
      .catch((e) => {
        if (!cancel) {
          setHintsErr(String(e));
          setHints(null);
        }
      })
      .finally(() => {
        if (!cancel) setHintsBusy(false);
      });
    return () => {
      cancel = true;
    };
  }, [base, live, app]);

  const queryTemporalAtIso = useCallback(
    async (iso: string, syncPicker: boolean) => {
      if (!live || !app) return;
      if (syncPicker) {
        const local = utcIsoToDatetimeLocalValue(iso);
        if (local) setAsOfLocal(local);
      }
      setBusyTemporal(true);
      setErrTemporal(null);
      setTemporal(null);
      try {
        const res = await fetch(
          `${base}/api/applications/${encodeURIComponent(app)}/compliance?as_of=${encodeURIComponent(iso)}`,
        );
        const body = (await res.json().catch(() => ({}))) as ComplianceResponse;
        if (!res.ok) {
          setErrTemporal(
            typeof body.detail === "string" ? body.detail : `${res.status}: ${JSON.stringify(body).slice(0, 280)}`,
          );
          return;
        }
        setTemporal(body);
      } catch (e) {
        setErrTemporal(String(e));
      } finally {
        setBusyTemporal(false);
      }
    },
    [base, live, app],
  );

  const queryAsOf = useCallback(async () => {
    if (!asOfLocal) return;
    const d = new Date(asOfLocal);
    if (Number.isNaN(d.getTime())) {
      setErrTemporal("Invalid date.");
      return;
    }
    await queryTemporalAtIso(d.toISOString(), false);
  }, [asOfLocal, queryTemporalAtIso]);

  const queryCurrent = useCallback(async () => {
    if (!live || !app) return;
    setBusyCurrent(true);
    setErrCurrent(null);
    setCurrent(null);
    try {
      const res = await fetch(`${base}/api/applications/${encodeURIComponent(app)}/compliance/current`);
      const body = (await res.json().catch(() => ({}))) as ComplianceResponse & { source?: string };
      if (!res.ok) {
        setErrCurrent(
          typeof body.detail === "string" ? body.detail : `${res.status}: ${JSON.stringify(body).slice(0, 280)}`,
        );
        return;
      }
      setCurrent(body);
    } catch (e) {
      setErrCurrent(String(e));
    } finally {
      setBusyCurrent(false);
    }
  }, [base, live, app]);

  const runStep3Demo = useCallback(async () => {
    const before = hints?.suggested_as_of_before_first_completion_utc;
    if (!before || !app || !live) return;
    await queryTemporalAtIso(before, true);
    await queryCurrent();
  }, [hints, app, live, queryTemporalAtIso, queryCurrent]);

  const beforeIso = hints?.suggested_as_of_before_first_completion_utc;
  const afterIso = hints?.suggested_as_of_after_first_completion_utc;
  const vTemporal = verdictSummary(temporal?.data as Record<string, unknown> | undefined);
  const vCurrent = verdictSummary(current?.data as Record<string, unknown> | undefined);
  const compare =
    temporal?.ok && current?.ok ? (vTemporal === vCurrent ? "Same headline verdict (expand raw if needed)." : "Differs — temporal vs current.") : null;

  if (!live) {
    return (
      <div className="rounded-2xl border border-white/[0.06] bg-apex-slate/10 p-6 text-sm text-apex-mist">
        Connect the staff API (dev proxy or <code className="text-apex-champagne">VITE_API_URL</code>) for temporal compliance.
      </div>
    );
  }

  const ledgerExample =
    app && beforeIso
      ? `ledger://${`applications/${app}/compliance?as_of=${beforeIso}`}`
      : `ledger://applications/{application_id}/compliance?as_of={ISO-8601}`;

  return (
    <div className="rounded-2xl border border-apex-gold/20 bg-apex-slate/15 p-6 md:p-8">
      <h2 className="font-display text-xl text-apex-ivory">Step 3 · Temporal compliance (Week 5)</h2>

      <div className="mt-4 rounded-xl border border-emerald-500/20 bg-emerald-950/10 p-4 text-sm leading-relaxed text-apex-cream">
        <p className="font-semibold text-emerald-200/90">In plain terms</p>
        <p className="mt-2 text-apex-mist">
          Compliance is not just “what the screen says today.” For audits and regulators you often need:{" "}
          <strong className="text-apex-ivory">“What did we know at a specific moment in the past?”</strong> This tab shows that:
          you pick a <em>clock time</em> (<span className="text-apex-champagne">as of</span>), and the system rebuilds the compliance
          picture using only information that had already happened by then. That can differ from “current” after more checks or
          decisions are recorded later. It proves the ledger is a truthful time machine for investigations — not a single mutable
          row you overwrite.
        </p>
      </div>

      <blockquote className="mt-4 border-l-2 border-apex-gold/50 pl-4 text-xs italic leading-relaxed text-apex-mist/90">
        Formal wording:{" "}
        {hints?.week5_prerequisite_step_3 ??
          "Query ledger://applications/{id}/compliance?as_of={timestamp} for a past point in time. Show the compliance state as it existed at that moment, distinct from the current state."}
      </blockquote>

      <div className="mt-4 rounded-xl border border-white/10 bg-black/25 p-4 text-[11px] leading-relaxed text-apex-mist">
        <p className="font-semibold text-apex-champagne">Same demo as the course (technical mirror)</p>
        <p className="mt-2">
          <span className="text-apex-mist/80">Resource style: </span>
          <code className="break-all text-apex-cream">{ledgerExample}</code>
        </p>
        <p className="mt-2">
          <span className="text-apex-mist/80">This API: </span>
          <code className="break-all text-apex-cream">
            GET /api/applications/{"{application_id}"}/compliance?as_of=
            {"{ISO-8601}"}
          </code>
        </p>
        <p className="mt-2 text-apex-mist/90">
          If a snapshot table is installed, that can answer instantly; otherwise the server replays stored events up to your{" "}
          <span className="text-apex-cream">as of</span> time — same outcome, different storage shortcut.
        </p>
      </div>

      <div className="mt-6">
        <label htmlFor="tc-application-id" className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">
          Application ID
        </label>
        <input
          id="tc-application-id"
          type="text"
          value={idDraft}
          onChange={(e) => setIdDraft(e.target.value)}
          placeholder="e.g. APEX-9756"
          autoComplete="off"
          spellCheck={false}
          className="mt-2 w-full max-w-md rounded-lg border border-white/15 bg-black/30 px-3 py-2 font-mono text-sm text-apex-cream placeholder:text-apex-mist/50 focus:border-apex-gold/40 focus:outline-none focus:ring-1 focus:ring-apex-gold/30"
        />
      </div>

      <div className="mt-6 rounded-xl border border-apex-gold/20 bg-apex-gold/5 p-4">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">Database-backed demo hints</p>
        {hintsBusy ? (
          <p className="mt-2 text-sm text-apex-mist">Reading your Postgres events for ComplianceCheckCompleted…</p>
        ) : hintsErr ? (
          <div className="mt-2 text-sm text-rose-100/90">
            <p>{hintsErr}</p>
            {hintsErr.includes("404") || hintsErr.toLowerCase().includes("compliancecheckcompleted") ? (
              <p className="mt-2 text-xs font-normal text-apex-mist">
                That usually means this application id has no <code className="text-apex-champagne/90">ComplianceCheckCompleted</code>{" "}
                events in Postgres yet. Try an id from your pipeline that has finished the compliance step, or run the workflow
                for this app.
              </p>
            ) : null}
          </div>
        ) : hints?.ok ? (
          <>
            <dl className="mt-3 space-y-3 text-[11px] text-apex-mist sm:grid sm:grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)] sm:gap-x-4">
              <dt className="text-apex-mist/70">When compliance finished (first completion on ledger)</dt>
              <dd>
                {hints.first_compliance_check_completed_recorded_at_utc ? (
                  <>
                    <div className="font-medium text-apex-cream">
                      {formatInstantForUi(hints.first_compliance_check_completed_recorded_at_utc).readable}
                    </div>
                    <div className="mt-0.5 break-all font-mono text-[9px] text-apex-mist/65">
                      {formatInstantForUi(hints.first_compliance_check_completed_recorded_at_utc).iso}
                    </div>
                  </>
                ) : (
                  "—"
                )}
              </dd>
              <dt className="text-apex-mist/70">Snapshot table</dt>
              <dd className="text-apex-cream">
                {hints.compliance_snapshot_table_available ? "Yes (may use saved snapshots)" : "No (replay from events — common in dev)"}
              </dd>
              <dt className="text-apex-mist/70">Try “before” compliance finished</dt>
              <dd>
                {beforeIso ? (
                  <>
                    <div className="font-medium text-apex-cream">{formatInstantForUi(beforeIso).readable}</div>
                    <div className="mt-0.5 break-all font-mono text-[9px] text-apex-mist/65">{beforeIso}</div>
                  </>
                ) : (
                  "—"
                )}
              </dd>
              <dt className="text-apex-mist/70">Try “after” compliance finished</dt>
              <dd>
                {afterIso ? (
                  <>
                    <div className="font-medium text-apex-cream">{formatInstantForUi(afterIso).readable}</div>
                    <div className="mt-0.5 break-all font-mono text-[9px] text-apex-mist/65">{afterIso}</div>
                  </>
                ) : (
                  "—"
                )}
              </dd>
            </dl>
            <div className="mt-4 flex flex-wrap gap-2">
              <button
                type="button"
                disabled={!beforeIso || busyTemporal}
                onClick={() => beforeIso && void queryTemporalAtIso(beforeIso, true)}
                className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-4 py-2 text-xs font-semibold text-apex-void disabled:opacity-40"
              >
                Query @ before (DB)
              </button>
              <button
                type="button"
                disabled={!afterIso || busyTemporal}
                onClick={() => afterIso && void queryTemporalAtIso(afterIso, true)}
                className="rounded-full border border-apex-gold/40 px-4 py-2 text-xs font-medium text-apex-gold disabled:opacity-40"
              >
                Query @ after (DB)
              </button>
              <button
                type="button"
                disabled={!beforeIso || busyTemporal || busyCurrent}
                onClick={() => void runStep3Demo()}
                className="rounded-full border border-white/20 px-4 py-2 text-xs text-apex-cream disabled:opacity-40"
              >
                Run Step 3 (before + current)
              </button>
            </div>
          </>
        ) : (
          <p className="mt-2 text-sm text-apex-mist">Enter an application id with compliance events.</p>
        )}
      </div>

      {compare ? (
        <p className="mt-4 text-xs text-apex-mist">
          <span className="font-semibold text-apex-champagne">Headline check: </span>
          temporal overall_verdict ≈ <span className="font-mono text-apex-cream">{vTemporal}</span>
          {" · "}
          current ≈ <span className="font-mono text-apex-cream">{vCurrent}</span>
          {" — "}
          {compare}
        </p>
      ) : null}

      <div className="mt-8 grid gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-apex-gold/25 bg-black/20 p-5">
          <h3 className="font-display text-sm font-semibold text-apex-gold">Past point in time</h3>
          <p className="mt-1 text-[11px] text-apex-mist">
            <code className="text-apex-champagne/80">GET /api/applications/…/compliance?as_of=</code>
          </p>
          <div className="mt-4 flex flex-wrap items-end gap-2">
            <div>
              <label htmlFor="tc-as-of" className="text-[10px] text-apex-mist/80">
                as_of (local picker)
              </label>
              <input
                id="tc-as-of"
                type="datetime-local"
                value={asOfLocal}
                onChange={(e) => setAsOfLocal(e.target.value)}
                className="mt-1 block rounded-lg border border-white/10 bg-apex-slate/40 px-3 py-2 text-xs text-apex-ivory"
              />
            </div>
            <button
              type="button"
              disabled={!app || !asOfLocal || busyTemporal}
              onClick={() => void queryAsOf()}
              className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-5 py-2 text-xs font-semibold text-apex-void disabled:opacity-40"
            >
              {busyTemporal ? "Querying…" : "Query compliance @ as_of"}
            </button>
          </div>
          {asOfLocal ? (
            <p className="mt-2 text-[11px] text-apex-mist">
              <span className="text-apex-mist/80">You are querying: </span>
              <span className="font-medium text-apex-cream">{formatInstantForUi(new Date(asOfLocal).toISOString()).readable}</span>
              <span className="mt-1 block font-mono text-[9px] text-apex-mist/65">
                ISO sent to API: {new Date(asOfLocal).toISOString()}
              </span>
            </p>
          ) : null}
          <div className="mt-4">
            <OutcomePanel
              title="Compliance @ as_of"
              subtitle="State reconstructed only from events (and snapshots) valid at or before this instant — not “now”."
              body={temporal}
              err={errTemporal}
              busy={busyTemporal}
            />
          </div>
        </div>

        <div className="rounded-xl border border-white/15 bg-black/20 p-5">
          <h3 className="font-display text-sm font-semibold text-apex-ivory">Current</h3>
          <p className="mt-1 text-[11px] text-apex-mist">
            <code className="text-apex-champagne/80">GET /api/applications/…/compliance/current</code>
          </p>
          <button
            type="button"
            disabled={!app || busyCurrent}
            onClick={() => void queryCurrent()}
            className="mt-4 rounded-full border border-apex-gold/40 px-5 py-2 text-xs font-medium text-apex-gold hover:bg-apex-gold/10 disabled:opacity-40"
          >
            {busyCurrent ? "Loading…" : "Load current compliance"}
          </button>
          <div className="mt-4">
            <OutcomePanel title="Current compliance view" body={current} err={errCurrent} busy={busyCurrent} />
          </div>
        </div>
      </div>
    </div>
  );
}
