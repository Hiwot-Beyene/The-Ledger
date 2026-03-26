import { useMemo, useState } from "react";
import { motion } from "framer-motion";
import { Link } from "react-router-dom";
import { useLedgerData } from "@/contexts/LedgerDataContext";
import OCCDemoTools from "@/components/OCCDemoTools";
import TemporalComplianceTools from "@/components/TemporalComplianceTools";
import UpcastImmutabilityTools from "@/components/UpcastImmutabilityTools";
import GasTownRecoveryTools from "@/components/GasTownRecoveryTools";
import { formatConfidence, formatDateTime } from "@/lib/formatDateTime";
import { isLiveApiMode, resolveApiBase } from "@/lib/apiBase";

type RawEvent = {
  event_id: string;
  stream_id: string;
  stream_position: number;
  global_position: number;
  event_type: string;
  event_version: number;
  metadata?: Record<string, unknown>;
  recorded_at: string;
  payload?: Record<string, unknown>;
};

type RegulatoryPackage = {
  package_version: string;
  application_id: string;
  examination_date: string;
  event_stream: RawEvent[];
  projection_states_as_of_examination_date: Record<string, Record<string, unknown>>;
  audit_integrity: {
    chain_valid: boolean;
    latest_previous_hash: string | null;
    computed_integrity_hash: string;
    hash_algorithm: string;
    verified_events_count: number;
  };
  narrative: string[];
  causal_links: {
    cause_event_id: string;
    effect_event_id: string;
    effect_event_type: string;
    effect_stream_id: string;
    correlation_id: string | null;
    link_kind?: string;
    payload_field?: string | null;
  }[];
  ai_decision_traceability: Record<string, unknown>[];
  package_sha256: string;
};

type ParsedMeta = {
  application_id: string;
  intent_matched: boolean;
  examination_at_from_query: string | null;
  signals: string[];
};

type HistoryResponse = {
  ok?: boolean;
  duration_ms?: number;
  examination_at?: string;
  package?: RegulatoryPackage;
  detail?: string;
  parsed?: ParsedMeta;
};

function metaStr(m: Record<string, unknown> | undefined, key: string): string {
  if (!m) return "—";
  const v = m[key];
  if (v == null || String(v).trim() === "") return "—";
  return String(v);
}

export default function NavigatorPage() {
  const { pipeline } = useLedgerData();
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [raw, setRaw] = useState<HistoryResponse | null>(null);
  const [integrityOut, setIntegrityOut] = useState<string | null>(null);
  const [integrityBusy, setIntegrityBusy] = useState(false);
  const [tab, setTab] = useState<
    "navigator" | "occ" | "compliance" | "upcast" | "gastown" | "whatif"
  >("navigator");

  const live = isLiveApiMode();
  const pkg = raw?.package;
  const resolvedId = (raw?.parsed?.application_id ?? pkg?.application_id ?? "").trim();
  const exampleNl = useMemo(() => {
    const id = pipeline[0]?.id;
    return id
      ? `Show me the complete decision history of application ID ${id}`
      : "Show me the complete decision history of application ID APEX-0001";
  }, [pipeline]);

  const humanEvents = useMemo(() => {
    if (!pkg?.event_stream) return [];
    return pkg.event_stream.filter((e) =>
      ["HumanReviewRequested", "HumanReviewCompleted"].includes(e.event_type),
    );
  }, [pkg]);

  const agentEvents = useMemo(() => {
    if (!pkg?.event_stream) return [];
    return pkg.event_stream.filter((e) => e.event_type === "AgentSessionCompleted");
  }, [pkg]);

  const complianceView = pkg?.projection_states_as_of_examination_date?.compliance_audit_view;
  const appSummary = pkg?.projection_states_as_of_examination_date?.application_summary;

  const runNavigatorQuery = async (override?: string) => {
    const q = (override ?? query).trim();
    if (q.length < 5) {
      setErr("Ask in a full sentence (at least a few words).");
      return;
    }
    setErr(null);
    setRaw(null);
    setIntegrityOut(null);
    setLoading(true);
    try {
      const res = await fetch(`${resolveApiBase()}/api/navigator/decision-history`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: q }),
      });
      const body = (await res.json().catch(() => ({}))) as HistoryResponse;
      if (!res.ok) {
        const msg =
          typeof body.detail === "string"
            ? body.detail
            : `${res.status}: ${JSON.stringify(body).slice(0, 200)}`;
        setErr(msg);
        return;
      }
      setRaw(body);
    } catch (e) {
      setErr(String(e));
    } finally {
      setLoading(false);
    }
  };

  const runIntegrity = async () => {
    const id = resolvedId;
    if (!id) return;
    setIntegrityBusy(true);
    setIntegrityOut(null);
    try {
      const res = await fetch(`${resolveApiBase()}/api/ledger/integrity-check`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ entity_type: "loan", entity_id: id }),
      });
      setIntegrityOut(JSON.stringify(await res.json().catch(() => ({})), null, 2));
    } catch (e) {
      setIntegrityOut(String(e));
    } finally {
      setIntegrityBusy(false);
    }
  };

  if (!live) {
    return (
      <div className="rounded-2xl border border-white/[0.06] bg-apex-slate/10 p-8 text-sm text-apex-mist">
        <Link to="/" className="text-sm text-apex-champagne hover:text-apex-ivory">
          ← Command
        </Link>
        <h1 className="mt-6 font-display text-2xl text-apex-ivory">Step 1 · Navigator</h1>
        <p className="mt-4">
          Run the staff API (dev proxy to <code className="text-apex-champagne">:8080</code> or set{" "}
          <code className="text-apex-champagne">VITE_API_URL</code>) to load full decision history packages.
        </p>
      </div>
    );
  }

  return (
    <div>
      <Link to="/" className="text-sm text-apex-mist hover:text-apex-champagne">
        ← Command
      </Link>

      <div className="mt-8 flex flex-wrap gap-2">
        <button
          type="button"
          onClick={() => setTab("navigator")}
          className={`rounded-full border px-4 py-2 text-xs font-medium transition-colors ${
            tab === "navigator"
              ? "border-apex-gold/50 bg-apex-gold/10 text-apex-gold"
              : "border-white/15 bg-apex-slate/10 text-apex-cream hover:border-apex-gold/30"
          }`}
        >
          Step 1 · Navigator
        </button>
        <button
          type="button"
          onClick={() => setTab("occ")}
          className={`rounded-full border px-4 py-2 text-xs font-medium transition-colors ${
            tab === "occ"
              ? "border-apex-gold/50 bg-apex-gold/10 text-apex-gold"
              : "border-white/15 bg-apex-slate/10 text-apex-cream hover:border-apex-gold/30"
          }`}
        >
          Step 2 · OCC
        </button>
        <button
          type="button"
          onClick={() => setTab("compliance")}
          className={`rounded-full border px-4 py-2 text-xs font-medium transition-colors ${
            tab === "compliance"
              ? "border-apex-gold/50 bg-apex-gold/10 text-apex-gold"
              : "border-white/15 bg-apex-slate/10 text-apex-cream hover:border-apex-gold/30"
          }`}
        >
          Step 3 · Compliance
        </button>
        <button
          type="button"
          onClick={() => setTab("upcast")}
          className={`rounded-full border px-4 py-2 text-xs font-medium transition-colors ${
            tab === "upcast"
              ? "border-apex-gold/50 bg-apex-gold/10 text-apex-gold"
              : "border-white/15 bg-apex-slate/10 text-apex-cream hover:border-apex-gold/30"
          }`}
        >
          Step 4 · Upcast
        </button>
        <button
          type="button"
          onClick={() => setTab("gastown")}
          className={`rounded-full border px-4 py-2 text-xs font-medium transition-colors ${
            tab === "gastown"
              ? "border-apex-gold/50 bg-apex-gold/10 text-apex-gold"
              : "border-white/15 bg-apex-slate/10 text-apex-cream hover:border-apex-gold/30"
          }`}
        >
          Step 5 · Gas Town
        </button>
        <button
          type="button"
          onClick={() => setTab("whatif")}
          className={`rounded-full border px-4 py-2 text-xs font-medium transition-colors ${
            tab === "whatif"
              ? "border-apex-gold/50 bg-apex-gold/10 text-apex-gold"
              : "border-white/15 bg-apex-slate/10 text-apex-cream hover:border-apex-gold/30"
          }`}
        >
          Step 6 · What-if
        </button>
      </div>

      {tab === "navigator" ? (
        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          className="mt-8 glass-panel rounded-2xl p-8"
        >
          <>
            <h1 className="font-display text-3xl text-apex-ivory">Step 1 · Navigator</h1>
            <p className="mt-2 max-w-3xl text-sm text-apex-mist">
              Ask in plain language for the full decision package: event stream, agent sessions, compliance (as of examination
              time if you include an ISO timestamp), human review, causal links, and cryptographic integrity — the API interprets
              intent and extracts the application id (synonym-based, not a single fixed phrase).
            </p>

            <div className="mt-8 space-y-3">
              <label className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">
                Natural language query
              </label>
              <textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
                    e.preventDefault();
                    void runNavigatorQuery();
                  }
                }}
                rows={4}
                className="w-full resize-y rounded-xl border border-white/10 bg-apex-slate/40 px-4 py-3 text-sm leading-relaxed text-apex-ivory placeholder:text-apex-mist/60"
                placeholder={exampleNl}
              />
              <div className="flex flex-wrap items-center gap-3">
                <button
                  type="button"
                  disabled={loading}
                  onClick={() => void runNavigatorQuery()}
                  className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-8 py-3 text-sm font-semibold text-apex-void shadow-lg shadow-apex-gold/15 disabled:opacity-40"
                >
                  {loading ? "Loading…" : "Run query"}
                </button>
                <button
                  type="button"
                  disabled={loading}
                  onClick={() => {
                    setQuery(exampleNl);
                    void runNavigatorQuery(exampleNl);
                  }}
                  className="rounded-full border border-white/15 px-5 py-2.5 text-xs text-apex-cream hover:border-apex-gold/30 disabled:opacity-40"
                >
                  Try example
                </button>
                <span className="text-[10px] text-apex-mist">Ctrl+Enter / ⌘+Enter to run</span>
              </div>
            </div>

            {err ? (
              <p className="mt-6 rounded-lg border border-rose-500/30 bg-rose-950/20 p-4 text-sm text-rose-100/90">
                {err}
              </p>
            ) : null}
          </>
        </motion.div>
      ) : null}

      {tab === "navigator" && pkg ? (
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.06 }}
          className="mt-10 space-y-10"
        >
          <section className="glass-panel rounded-2xl p-8">
            <h2 className="font-display text-xl text-apex-ivory">Package & verification</h2>
            {raw?.parsed?.signals?.length ? (
              <p className="mt-3 text-[10px] text-apex-mist">
                Parse signals:{" "}
                <span className="font-mono text-apex-champagne/90">{raw.parsed.signals.join(" · ")}</span>
              </p>
            ) : null}
            <dl className="mt-4 grid gap-3 text-sm sm:grid-cols-2">
              <div>
                <dt className="text-apex-mist">Resolved application</dt>
                <dd className="font-mono text-apex-gold">{resolvedId || "—"}</dd>
              </div>
              <div>
                <dt className="text-apex-mist">Examination (from query)</dt>
                <dd className="font-mono text-apex-cream">
                  {raw?.parsed?.examination_at_from_query ?? "—"}
                </dd>
              </div>
              <div>
                <dt className="text-apex-mist">Examination (response)</dt>
                <dd className="font-mono text-apex-cream">{raw?.examination_at ?? "—"}</dd>
              </div>
              <div>
                <dt className="text-apex-mist">Round-trip</dt>
                <dd className="text-apex-cream">{raw?.duration_ms != null ? `${raw.duration_ms} ms` : "—"}</dd>
              </div>
              <div>
                <dt className="text-apex-mist">Package SHA-256</dt>
                <dd className="break-all font-mono text-[11px] text-apex-champagne">{pkg.package_sha256}</dd>
              </div>
              <div>
                <dt className="text-apex-mist">Integrity chain (from package)</dt>
                <dd className="text-apex-cream">
                  {pkg.audit_integrity.chain_valid ? "chain_valid ✓" : "chain_valid ✗"} · verified_events_count{" "}
                  {pkg.audit_integrity.verified_events_count} · algorithm {pkg.audit_integrity.hash_algorithm}
                </dd>
              </div>
              <div className="sm:col-span-2">
                <dt className="text-apex-mist">Computed rolling hash (package)</dt>
                <dd className="break-all font-mono text-[11px] text-apex-cream">
                  {pkg.audit_integrity.computed_integrity_hash}
                </dd>
              </div>
            </dl>
            <div className="mt-6 flex flex-wrap gap-3">
              <button
                type="button"
                disabled={integrityBusy || !resolvedId}
                onClick={() => void runIntegrity()}
                className="rounded-full border border-apex-gold/40 px-5 py-2 text-xs font-medium text-apex-gold hover:bg-apex-gold/10 disabled:opacity-40"
              >
                {integrityBusy ? "…" : "Run integrity check (server + audit event)"}
              </button>
              {resolvedId ? (
                <Link
                  to={`/applications/${encodeURIComponent(resolvedId)}`}
                  className="rounded-full border border-white/15 px-5 py-2 text-xs text-apex-cream hover:border-apex-gold/30"
                >
                  Open dossier
                </Link>
              ) : null}
            </div>
            {integrityOut ? (
              <pre className="mt-4 max-h-56 overflow-auto rounded-lg border border-white/10 bg-black/40 p-3 text-[10px] text-apex-cream">
                {integrityOut}
              </pre>
            ) : null}
          </section>

          {appSummary && Object.keys(appSummary).length > 0 ? (
            <section className="glass-panel rounded-2xl p-8">
              <h2 className="font-display text-xl text-apex-ivory">Projection snapshot (as of examination)</h2>
              <p className="mt-1 text-xs text-apex-mist">application_summary replay</p>
              <pre className="mt-4 overflow-auto rounded-lg border border-white/10 bg-black/30 p-4 text-[11px] text-apex-cream">
                {JSON.stringify(appSummary, null, 2)}
              </pre>
            </section>
          ) : null}

          <section className="glass-panel rounded-2xl p-8">
            <h2 className="font-display text-xl text-apex-ivory">Narrative</h2>
            <ul className="mt-4 list-inside list-disc space-y-2 text-sm text-apex-cream">
              {pkg.narrative.map((line, i) => (
                <li key={i}>{line}</li>
              ))}
            </ul>
          </section>

          <section className="glass-panel rounded-2xl p-8">
            <h2 className="font-display text-xl text-apex-ivory">Event stream</h2>
            <p className="mt-1 text-xs text-apex-mist">{pkg.event_stream.length} events · ordered by global position</p>
            <div className="mt-4 overflow-x-auto rounded-lg border border-white/[0.08]">
              <table className="w-full min-w-[900px] text-left text-[11px]">
                <thead className="border-b border-white/10 bg-apex-slate/30 text-apex-mist">
                  <tr>
                    <th className="p-2 font-medium">g#</th>
                    <th className="p-2 font-medium">stream</th>
                    <th className="p-2 font-medium">type</th>
                    <th className="p-2 font-medium">recorded</th>
                    <th className="p-2 font-medium">correlation</th>
                    <th className="p-2 font-medium">causation</th>
                  </tr>
                </thead>
                <tbody className="text-apex-cream">
                  {pkg.event_stream.map((e) => (
                    <tr key={e.event_id} className="border-b border-white/[0.04]">
                      <td className="p-2 font-mono text-apex-champagne">{e.global_position}</td>
                      <td className="max-w-[140px] truncate p-2 font-mono text-apex-mist">{e.stream_id}</td>
                      <td className="p-2 font-mono">{e.event_type}</td>
                      <td className="p-2 text-apex-mist">{formatDateTime(e.recorded_at)}</td>
                      <td className="max-w-[120px] truncate p-2 font-mono text-apex-mist">
                        {metaStr(e.metadata, "correlation_id")}
                      </td>
                      <td className="max-w-[120px] truncate p-2 font-mono text-apex-mist">
                        {metaStr(e.metadata, "causation_id")}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="glass-panel rounded-2xl p-8">
            <h2 className="font-display text-xl text-apex-ivory">Agent actions</h2>
            <p className="mt-1 text-xs text-apex-mist">
              AgentSessionCompleted on stream · aggregate: agent_performance_ledger
            </p>
            {pkg.projection_states_as_of_examination_date?.agent_performance_ledger ? (
              <pre className="mt-4 overflow-auto rounded-lg border border-white/10 bg-black/30 p-4 text-[11px] text-apex-cream">
                {JSON.stringify(pkg.projection_states_as_of_examination_date.agent_performance_ledger, null, 2)}
              </pre>
            ) : null}
            {agentEvents.length === 0 ? (
              <p className="mt-4 text-sm text-apex-mist">No agent session completion events in this package.</p>
            ) : (
              <ul className="mt-4 space-y-4">
                {agentEvents.map((e) => (
                  <li key={e.event_id} className="rounded-lg border border-white/[0.06] bg-apex-slate/20 p-4">
                    <p className="font-mono text-[11px] text-apex-gold">{e.event_id}</p>
                    <p className="mt-1 text-xs text-apex-mist">
                      {formatDateTime(e.recorded_at)} · {e.stream_id} #{e.stream_position}
                    </p>
                    <pre className="mt-2 max-h-40 overflow-auto text-[10px] text-apex-cream">
                      {JSON.stringify(e.payload ?? {}, null, 2)}
                    </pre>
                  </li>
                ))}
              </ul>
            )}
          </section>

          <section className="glass-panel rounded-2xl p-8">
            <h2 className="font-display text-xl text-apex-ivory">Compliance checks</h2>
            {!complianceView || Object.keys(complianceView).length === 0 ? (
              <p className="mt-4 text-sm text-apex-mist">No compliance projection state at this examination time.</p>
            ) : (
              <pre className="mt-4 overflow-auto rounded-lg border border-white/10 bg-black/30 p-4 text-[11px] text-apex-cream">
                {JSON.stringify(complianceView, null, 2)}
              </pre>
            )}
          </section>

          <section className="glass-panel rounded-2xl p-8">
            <h2 className="font-display text-xl text-apex-ivory">Human review</h2>
            {humanEvents.length === 0 ? (
              <p className="mt-4 text-sm text-apex-mist">No HumanReviewRequested / HumanReviewCompleted in stream.</p>
            ) : (
              <ul className="mt-4 space-y-4">
                {humanEvents.map((e) => (
                  <li key={e.event_id} className="rounded-lg border border-white/[0.06] bg-apex-slate/20 p-4">
                    <p className="font-medium text-apex-ivory">{e.event_type}</p>
                    <p className="mt-1 font-mono text-[10px] text-apex-mist">{e.event_id}</p>
                    <p className="mt-1 text-xs text-apex-mist">{formatDateTime(e.recorded_at)}</p>
                    <pre className="mt-2 max-h-48 overflow-auto text-[10px] text-apex-cream">
                      {JSON.stringify(e.payload ?? {}, null, 2)}
                    </pre>
                  </li>
                ))}
              </ul>
            )}
          </section>

          <section className="glass-panel rounded-2xl p-8">
            <h2 className="font-display text-xl text-apex-ivory">Causal links</h2>
            <p className="mt-1 text-xs text-apex-mist">
              metadata.causation_id → prior event_id when agents use event causation; otherwise session_anchor when causation_id
              is an agent session_id (resolved to the first event in this bundle with that payload.session_id); payload refs
              (e.g. triggered_by_event_id) included when they resolve the same way.
            </p>
            {pkg.causal_links.length === 0 ? (
              <p className="mt-4 text-sm text-apex-mist">No causation edges between stored event IDs.</p>
            ) : (
              <ul className="mt-4 space-y-2 text-sm text-apex-cream">
                {pkg.causal_links.map((l, i) => (
                  <li key={`${l.effect_event_id}-${i}`} className="font-mono text-[11px]">
                    <span className="text-apex-champagne">{l.cause_event_id}</span>
                    <span className="text-apex-mist"> → </span>
                    <span>{l.effect_event_type}</span>
                    <span className="text-apex-mist"> ({l.effect_event_id.slice(0, 8)}…)</span>
                    {l.link_kind ? (
                      <span className="ml-2 rounded border border-white/10 px-1.5 text-[10px] text-apex-mist">
                        {l.link_kind}
                        {l.payload_field ? `·${l.payload_field}` : ""}
                      </span>
                    ) : null}
                    {l.correlation_id ? (
                      <span className="ml-2 text-apex-mist">corr {String(l.correlation_id).slice(0, 12)}…</span>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </section>

          <section className="glass-panel rounded-2xl p-8">
            <h2 className="font-display text-xl text-apex-ivory">AI decision traceability</h2>
            {pkg.ai_decision_traceability.length === 0 ? (
              <p className="mt-4 text-sm text-apex-mist">No model hash / confidence fields extracted.</p>
            ) : (
              <div className="mt-4 overflow-x-auto rounded-lg border border-white/[0.08]">
                <table className="w-full text-left text-[11px]">
                  <thead className="border-b border-white/10 bg-apex-slate/30 text-apex-mist">
                    <tr>
                      <th className="p-2">event_type</th>
                      <th className="p-2">model</th>
                      <th className="p-2">confidence</th>
                      <th className="p-2">input_data_hash</th>
                      <th className="p-2">recorded</th>
                    </tr>
                  </thead>
                  <tbody className="text-apex-cream">
                    {pkg.ai_decision_traceability.map((row) => (
                      <tr key={String(row.event_id)} className="border-b border-white/[0.04]">
                        <td className="p-2 font-mono">{String(row.event_type)}</td>
                        <td className="p-2 font-mono text-apex-mist">
                          {row.model_version != null
                            ? String(row.model_version)
                            : row.model_versions != null
                              ? JSON.stringify(row.model_versions)
                              : "—"}
                        </td>
                        <td className="p-2">{formatConfidence(row.confidence_score)}</td>
                        <td className="max-w-[160px] truncate p-2 font-mono text-apex-mist">
                          {row.input_data_hash != null ? String(row.input_data_hash) : "—"}
                        </td>
                        <td className="p-2 text-apex-mist">{formatDateTime(String(row.recorded_at ?? ""))}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </motion.div>
      ) : null}

      {tab === "occ" ? (
        <div className="mt-10">
          <OCCDemoTools applicationId={resolvedId || pipeline[0]?.id || ""} />
        </div>
      ) : null}

      {tab === "compliance" ? (
        <div className="mt-10">
          <TemporalComplianceTools applicationId={resolvedId || pipeline[0]?.id || ""} />
        </div>
      ) : null}

      {tab === "upcast" ? (
        <div className="mt-10">
          <UpcastImmutabilityTools applicationId={resolvedId || pipeline[0]?.id || ""} />
        </div>
      ) : null}

      {tab === "gastown" ? (
        <motion.div className="mt-10" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}>
          <GasTownRecoveryTools applicationId={resolvedId || pipeline[0]?.id || ""} />
        </motion.div>
      ) : null}

      {tab === "whatif" ? (
        <motion.div className="mt-10" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}>
          <WhatIfCounterfactualTools applicationId={resolvedId || pipeline[0]?.id || ""} />
        </motion.div>
      ) : null}
    </div>
  );
}
