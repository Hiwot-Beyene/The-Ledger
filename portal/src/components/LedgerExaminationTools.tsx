import { useCallback, useEffect, useState } from "react";
import { isLiveApiMode, resolveApiBase } from "@/lib/apiBase";

type Props = { applicationId: string };

function JsonBlock({ label, value }: { label: string; value: string }) {
  return (
    <div className="mt-4">
      <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">{label}</p>
      <pre className="mt-2 max-h-64 overflow-auto rounded-lg border border-white/10 bg-black/40 p-3 text-[10px] text-apex-cream">
        {value}
      </pre>
    </div>
  );
}

export default function LedgerExaminationTools({ applicationId }: Props) {
  const base = resolveApiBase();
  const [busy, setBusy] = useState<string | null>(null);
  const [decisionHistory, setDecisionHistory] = useState<string>("");
  const [occ, setOcc] = useState<string>("");
  const [compliance, setCompliance] = useState<string>("");
  const [complianceAsOf, setComplianceAsOf] = useState<string>("");
  const [integrity, setIntegrity] = useState<string>("");
  const [upcast, setUpcast] = useState<string>("");
  const [upStream, setUpStream] = useState<string>(() => `loan-${applicationId}`);
  const [upPos, setUpPos] = useState<string>("4");
  const [gasAgent, setGasAgent] = useState("document_processing");
  const [gasSess, setGasSess] = useState("");
  const [gasOut, setGasOut] = useState<string>("");
  const [whatIf, setWhatIf] = useState<string>("");

  useEffect(() => {
    setUpStream(`loan-${applicationId}`);
  }, [applicationId]);

  const run = useCallback(
    async (key: string, fn: () => Promise<void>) => {
      if (!isLiveApiMode()) return;
      setBusy(key);
      try {
        await fn();
      } finally {
        setBusy(null);
      }
    },
    [],
  );

  if (!isLiveApiMode()) {
    return (
      <div className="rounded-2xl border border-white/[0.06] bg-apex-slate/10 p-6 text-sm text-apex-mist">
        Connect the staff API (dev proxy or <code className="text-apex-champagne">VITE_API_URL</code>) to use regulatory
        examination tools.
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-apex-gold/20 bg-apex-slate/15 p-6 md:p-8">
      <h2 className="font-display text-xl text-apex-ivory">Ledger examination</h2>
      <p className="mt-1 text-xs text-apex-mist">
        Same contracts as MCP/time-travel: decision package, append concurrency proof on the loan stream, compliance{" "}
        <code className="text-apex-champagne/90">as_of</code>, integrity chain append, upcast proof, Gas Town replay,
        what-if replay.
      </p>

      <div className="mt-6 flex flex-wrap gap-2">
        <button
          type="button"
          disabled={!!busy}
          onClick={() =>
            run("dh", async () => {
              const t0 = performance.now();
              const res = await fetch(`${base}/api/applications/${encodeURIComponent(applicationId)}/audit/decision-history`);
              const j = await res.json().catch(() => ({}));
              const ms = Math.round(performance.now() - t0);
              setDecisionHistory(JSON.stringify({ http_status: res.status, client_roundtrip_ms: ms, ...j }, null, 2));
            })
          }
          className="rounded-full border border-apex-gold/40 px-4 py-2 text-xs font-medium text-apex-gold hover:bg-apex-gold/10 disabled:opacity-40"
        >
          {busy === "dh" ? "…" : "1 · Decision history package"}
        </button>
        <button
          type="button"
          disabled={!!busy}
          onClick={() =>
            run("int", async () => {
              const res = await fetch(`${base}/api/ledger/integrity-check`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ entity_type: "loan", entity_id: applicationId }),
              });
              setIntegrity(JSON.stringify(await res.json().catch(() => ({})), null, 2));
            })
          }
          className="rounded-full border border-white/15 px-4 py-2 text-xs text-apex-cream hover:border-apex-gold/30 disabled:opacity-40"
        >
          Integrity check (+ audit event)
        </button>
        <button
          type="button"
          disabled={!!busy}
          onClick={() =>
            run("occ", async () => {
              const res = await fetch(
                `${base}/api/applications/${encodeURIComponent(applicationId)}/ledger/append-concurrency-proof`,
                { method: "POST" },
              );
              setOcc(JSON.stringify(await res.json().catch(() => ({})), null, 2));
            })
          }
          className="rounded-full border border-apex-gold/40 px-4 py-2 text-xs font-medium text-apex-gold hover:bg-apex-gold/10 disabled:opacity-40"
        >
          {busy === "occ" ? "…" : "2 · Append concurrency proof"}
        </button>
      </div>
      {(decisionHistory || integrity) && (
        <div className="mt-2 grid gap-4 md:grid-cols-2">
          {decisionHistory ? <JsonBlock label="Decision history + package timing" value={decisionHistory} /> : null}
          {integrity ? <JsonBlock label="Integrity check result" value={integrity} /> : null}
        </div>
      )}
      {occ ? <JsonBlock label="Optimistic concurrency (loan-{application_id} stream)" value={occ} /> : null}

      <div className="mt-8 border-t border-white/[0.06] pt-6">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">3 · Temporal compliance</p>
        <div className="mt-2 flex flex-wrap items-end gap-2">
          <input
            type="datetime-local"
            value={complianceAsOf}
            onChange={(e) => setComplianceAsOf(e.target.value)}
            className="rounded-lg border border-white/10 bg-apex-slate/40 px-3 py-2 text-xs text-apex-ivory"
          />
          <button
            type="button"
            disabled={!!busy || !complianceAsOf}
            onClick={() =>
              run("cmp", async () => {
                const d = new Date(complianceAsOf);
                const iso = d.toISOString();
                const res = await fetch(
                  `${base}/api/applications/${encodeURIComponent(applicationId)}/compliance?as_of=${encodeURIComponent(iso)}`,
                );
                setCompliance(JSON.stringify(await res.json().catch(() => ({})), null, 2));
              })
            }
            className="rounded-full bg-apex-gold/15 px-4 py-2 text-xs font-medium text-apex-gold disabled:opacity-40"
          >
            Query compliance @ as_of
          </button>
        </div>
        <p className="mt-1 text-[10px] text-apex-mist/80">
          Mirrors <code className="text-apex-champagne/80">ledger://applications/.../compliance?as_of=</code> — snapshot
          table if present, else event replay.
        </p>
        {compliance ? <JsonBlock label="Compliance state" value={compliance} /> : null}
      </div>

      <div className="mt-8 border-t border-white/[0.06] pt-6">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">4 · Upcast proof</p>
        <div className="mt-2 flex flex-wrap gap-2">
          <input
            value={upStream}
            onChange={(e) => setUpStream(e.target.value)}
            placeholder="stream_id"
            className="min-w-[200px] flex-1 rounded-lg border border-white/10 bg-apex-slate/40 px-3 py-2 font-mono text-xs text-apex-ivory"
          />
          <input
            value={upPos}
            onChange={(e) => setUpPos(e.target.value)}
            placeholder="position"
            className="w-24 rounded-lg border border-white/10 bg-apex-slate/40 px-3 py-2 font-mono text-xs text-apex-ivory"
          />
          <button
            type="button"
            disabled={!!busy}
            onClick={() =>
              run("up", async () => {
                const res = await fetch(
                  `${base}/api/streams/${encodeURIComponent(upStream)}/events/${encodeURIComponent(upPos)}/upcast-proof`,
                );
                setUpcast(JSON.stringify(await res.json().catch(() => ({})), null, 2));
              })
            }
            className="rounded-full border border-white/15 px-4 py-2 text-xs text-apex-cream"
          >
            Compare DB row vs store load
          </button>
        </div>
        {upcast ? <JsonBlock label="Raw row vs upcasted" value={upcast} /> : null}
      </div>

      <div className="mt-8 border-t border-white/[0.06] pt-6">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">5 · Gas Town reconstruct</p>
        <div className="mt-2 flex flex-wrap gap-2">
          <input
            value={gasAgent}
            onChange={(e) => setGasAgent(e.target.value)}
            placeholder="agent_id"
            className="rounded-lg border border-white/10 bg-apex-slate/40 px-3 py-2 text-xs text-apex-ivory"
          />
          <input
            value={gasSess}
            onChange={(e) => setGasSess(e.target.value)}
            placeholder="session_id (from agent-* stream)"
            className="min-w-[220px] flex-1 rounded-lg border border-white/10 bg-apex-slate/40 px-3 py-2 font-mono text-xs text-apex-ivory"
          />
          <button
            type="button"
            disabled={!!busy || !gasSess}
            onClick={() =>
              run("gas", async () => {
                const res = await fetch(`${base}/api/ledger/reconstruct-agent-context`, {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({ agent_id: gasAgent, session_id: gasSess }),
                });
                setGasOut(JSON.stringify(await res.json().catch(() => ({})), null, 2));
              })
            }
            className="rounded-full border border-white/15 px-4 py-2 text-xs text-apex-cream"
          >
            reconstruct_agent_context
          </button>
        </div>
        {gasOut ? <JsonBlock label="Reconstructed context" value={gasOut} /> : null}
      </div>

      <div className="mt-8 border-t border-white/[0.06] pt-6">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">
          6 · What-if (credit branch)
        </p>
        <p className="mt-1 text-[10px] text-apex-mist/80">
          Substitute a counterfactual <code className="text-apex-champagne/80">CreditAnalysisCompleted</code> payload;
          replays projections (see divergences on decision).
        </p>
        <button
          type="button"
          disabled={!!busy}
          onClick={() =>
            run("wi", async () => {
              const res = await fetch(`${base}/api/ledger/what-if`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  application_id: applicationId,
                  branch_at_event_type: "CreditAnalysisCompleted",
                  counterfactual_events: [
                    {
                      event_type: "CreditAnalysisCompleted",
                      event_version: 1,
                      payload: {
                        application_id: applicationId,
                        agent_id: "credit_whatif",
                        session_id: "sess-whatif",
                        model_version: "demo-v1",
                        confidence_score: 0.72,
                        risk_tier: "HIGH",
                        recommended_limit_usd: "250000",
                        duration_ms: 1,
                        completed_at: new Date().toISOString(),
                      },
                    },
                  ],
                }),
              });
              setWhatIf(JSON.stringify(await res.json().catch(() => ({})), null, 2));
            })
          }
          className="mt-2 rounded-full border border-apex-gold/40 px-4 py-2 text-xs font-medium text-apex-gold"
        >
          Run HIGH vs recorded MEDIUM (example payload)
        </button>
        {whatIf ? <JsonBlock label="What-if outcome" value={whatIf} /> : null}
      </div>
    </div>
  );
}
