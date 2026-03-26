import { useCallback, useEffect, useState } from "react";
import { motion } from "framer-motion";
import { isLiveApiMode, resolveApiBase } from "@/lib/apiBase";

type Props = { applicationId?: string };

type Summary = Record<string, unknown>;

type WhatIfPayload = {
  ok?: boolean;
  tier_before?: string | null;
  branch_event_id?: string;
  real_application_summary?: Summary;
  counterfactual_application_summary?: Summary;
  application_summary_field_diff?: Record<string, { real: unknown; counterfactual: unknown }>;
  divergence_events?: { reason: string; event_type: string; event_id: string; stream_id: string }[];
  causal_policy?: {
    decision_events_excluded?: boolean;
    application_approved_excluded?: boolean;
    dependent_events_dropped_count?: number;
  };
  examiner_notes?: string[];
  stale_downstream_warning?: string | null;
  detail?: string;
};

export default function WhatIfCounterfactualTools({ applicationId = "" }: Props) {
  const live = isLiveApiMode();
  const base = resolveApiBase();
  const [appId, setAppId] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [hint, setHint] = useState<{ sample_application_id_medium_credit?: string; narrative?: string } | null>(null);
  const [result, setResult] = useState<WhatIfPayload | null>(null);

  useEffect(() => {
    setAppId(applicationId.trim() || "APEX-0020");
  }, [applicationId]);

  useEffect(() => {
    if (!live) return;
    const aid = applicationId.trim() || "APEX-0020";
    let cancelled = false;
    void (async () => {
      try {
        const res = await fetch(`${base}/api/applications/${encodeURIComponent(aid)}/ledger/what-if-medium-to-high/hint`);
        const body = (await res.json().catch(() => ({}))) as typeof hint;
        if (!cancelled && res.ok) setHint(body);
      } catch {
        /* optional */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [base, live, applicationId]);

  const run = useCallback(async () => {
    if (!live) return;
    const id = appId.trim();
    if (!id) {
      setErr("Enter application_id.");
      return;
    }
    setBusy(true);
    setErr(null);
    setResult(null);
    try {
      const res = await fetch(
        `${base}/api/applications/${encodeURIComponent(id)}/ledger/what-if-medium-to-high`,
        { method: "POST" },
      );
      const body = (await res.json().catch(() => ({}))) as WhatIfPayload;
      if (!res.ok) {
        setErr(typeof body.detail === "string" ? body.detail : `${res.status}: ${JSON.stringify(body).slice(0, 400)}`);
        return;
      }
      setResult(body);
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  }, [base, live, appId]);

  if (!live) {
    return (
      <div className="rounded-2xl border border-white/[0.06] bg-apex-slate/10 p-6 text-sm text-apex-mist">
        Connect the staff API for what-if projections.
      </div>
    );
  }

  const diff = result?.application_summary_field_diff;

  return (
    <motion.div
      className="glass-panel rounded-2xl p-8"
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: [0.22, 1, 0.36, 1] }}
    >
      <h1 className="font-display text-3xl text-apex-ivory">Step 6 · What-if (bonus)</h1>
      <p className="mt-3 max-w-3xl text-sm text-apex-mist">
        Read-only replay: the first <code className="font-mono text-apex-champagne">CreditAnalysisCompleted</code> is replaced
        in memory with a <strong className="text-apex-ivory">HIGH</strong> tier and a tighter modeled limit. Events that
        causally depend on the original credit cell are dropped from the counterfactual tape so an old{" "}
        <code className="text-apex-champagne">DecisionGenerated</code> / approval is not applied on top of the alternate credit
        outcome — that is the enforcement / audit rule being demonstrated.
      </p>

      {hint?.narrative ? (
        <aside className="mt-4 rounded-xl border border-violet-500/20 bg-violet-950/15 p-4 text-xs text-apex-cream">
          <p className="font-semibold text-apex-ivory">Hint</p>
          <p className="mt-2 text-apex-mist">{hint.narrative}</p>
          {hint.sample_application_id_medium_credit ? (
            <button
              type="button"
              onClick={() => setAppId(hint.sample_application_id_medium_credit!)}
              className="mt-3 rounded-full border border-white/15 px-4 py-1.5 text-[11px] text-apex-champagne hover:border-apex-gold/35"
            >
              Use seed example {hint.sample_application_id_medium_credit}
            </button>
          ) : null}
        </aside>
      ) : null}

      <div className="mt-6 flex flex-wrap items-end gap-4">
        <label className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">
          application_id
          <input
            value={appId}
            onChange={(e) => setAppId(e.target.value)}
            className="mt-2 block w-64 rounded-xl border border-white/10 bg-apex-slate/40 px-3 py-2 font-mono text-xs text-apex-ivory"
          />
        </label>
        <button
          type="button"
          disabled={busy}
          onClick={() => void run()}
          className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-6 py-2.5 text-xs font-semibold text-apex-void shadow-lg disabled:opacity-40"
        >
          {busy ? "Projecting…" : "Run MEDIUM → HIGH counterfactual"}
        </button>
      </div>

      {err ? (
        <p className="mt-4 rounded-lg border border-rose-500/30 bg-rose-950/20 p-4 text-sm text-rose-100/90">{err}</p>
      ) : null}

      {result?.ok ? (
        <div className="mt-8 space-y-6">
          {result.stale_downstream_warning ? (
            <div className="rounded-xl border border-amber-500/30 bg-amber-950/15 p-4 text-xs text-amber-100/90">
              {result.stale_downstream_warning}
            </div>
          ) : null}

          {result.examiner_notes?.length ? (
            <ul className="list-inside list-disc space-y-1 text-xs text-apex-mist">
              {result.examiner_notes.map((n, i) => (
                <li key={i}>{n}</li>
              ))}
            </ul>
          ) : null}

          <div className="grid gap-4 lg:grid-cols-2">
            <div className="rounded-xl border border-white/[0.08] bg-black/25 p-4">
              <p className="text-xs font-semibold text-apex-ivory">Real application_summary (end of replay)</p>
              <p className="mt-1 text-[10px] text-apex-mist">
                tier before branch: <span className="font-mono text-apex-champagne">{String(result.tier_before ?? "—")}</span>
              </p>
              <pre className="mt-2 max-h-72 overflow-auto text-[10px] text-apex-cream">
                {JSON.stringify(result.real_application_summary, null, 2)}
              </pre>
            </div>
            <div className="rounded-xl border border-emerald-500/20 bg-emerald-950/10 p-4">
              <p className="text-xs font-semibold text-emerald-100/90">Counterfactual application_summary</p>
              <pre className="mt-2 max-h-72 overflow-auto text-[10px] text-apex-cream">
                {JSON.stringify(result.counterfactual_application_summary, null, 2)}
              </pre>
            </div>
          </div>

          {result.causal_policy ? (
            <dl className="grid gap-2 rounded-xl border border-white/[0.06] bg-apex-slate/20 p-4 text-[11px] text-apex-cream sm:grid-cols-3">
              <div>
                <dt className="text-apex-mist">Decision excluded</dt>
                <dd className="font-mono text-apex-gold">{String(result.causal_policy.decision_events_excluded)}</dd>
              </div>
              <div>
                <dt className="text-apex-mist">ApplicationApproved excluded</dt>
                <dd className="font-mono">{String(result.causal_policy.application_approved_excluded)}</dd>
              </div>
              <div>
                <dt className="text-apex-mist">Causally dropped events</dt>
                <dd className="font-mono">{String(result.causal_policy.dependent_events_dropped_count)}</dd>
              </div>
            </dl>
          ) : null}

          {diff && Object.keys(diff).length > 0 ? (
            <div className="rounded-xl border border-white/[0.08] p-4">
              <p className="text-xs font-semibold text-apex-ivory">Field-level divergence</p>
              <table className="mt-3 w-full text-left text-[11px]">
                <thead className="border-b border-white/10 text-apex-mist">
                  <tr>
                    <th className="p-2">field</th>
                    <th className="p-2">real</th>
                    <th className="p-2">counterfactual</th>
                  </tr>
                </thead>
                <tbody className="font-mono text-apex-cream">
                  {Object.entries(diff).map(([k, v]) => (
                    <tr key={k} className="border-b border-white/[0.04]">
                      <td className="p-2 text-apex-champagne">{k}</td>
                      <td className="max-w-[180px] truncate p-2">{JSON.stringify(v.real)}</td>
                      <td className="max-w-[180px] truncate p-2">{JSON.stringify(v.counterfactual)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}

          {result.divergence_events?.length ? (
            <div>
              <p className="text-xs font-semibold text-apex-ivory">Divergence / policy trace</p>
              <ul className="mt-2 space-y-1 font-mono text-[10px] text-apex-mist">
                {result.divergence_events.map((d, i) => (
                  <li key={`${d.event_id}-${i}`}>
                    <span className="text-apex-champagne">{d.reason}</span> · {d.event_type} ·{" "}
                    <span className="text-apex-cream">{d.stream_id}</span>
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </div>
      ) : null}
    </motion.div>
  );
}
