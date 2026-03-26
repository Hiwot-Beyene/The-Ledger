import { useCallback, useEffect, useState } from "react";
import { motion } from "framer-motion";
import { isLiveApiMode, resolveApiBase } from "@/lib/apiBase";

type Props = { applicationId?: string };

type SeedResponse = {
  ok?: boolean;
  stream_id?: string;
  session_id?: string;
  agent_id?: string;
  application_id?: string;
  events_appended?: number;
  last_stream_position?: number;
  narrative?: string[];
  detail?: string;
};

type ReconstructResponse = {
  ok?: boolean;
  stream_id?: string;
  session_id?: string;
  agent_id?: string;
  last_event_position?: number;
  session_health_status?: string;
  pending_work?: string[];
  context_text?: string;
  tail_event_types?: string[];
  persisted_event_count?: number;
  detail?: string;
};

export default function GasTownRecoveryTools({ applicationId = "" }: Props) {
  const live = isLiveApiMode();
  const base = resolveApiBase();
  const [appId, setAppId] = useState("");
  const [agentId, setAgentId] = useState("credit_analysis");
  const [sessionId, setSessionId] = useState("");
  const [seedBusy, setSeedBusy] = useState(false);
  const [recBusy, setRecBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [seedOut, setSeedOut] = useState<SeedResponse | null>(null);
  const [recOut, setRecOut] = useState<ReconstructResponse | null>(null);
  const [hint, setHint] = useState<{ step?: string; expected_signals?: string[] } | null>(null);

  useEffect(() => {
    const a = applicationId.trim();
    setAppId(a || "APEX-GAS-DEMO");
  }, [applicationId]);

  useEffect(() => {
    if (!live) return;
    let cancelled = false;
    void (async () => {
      try {
        const res = await fetch(`${base}/api/ledger/gas-town-demo-hint`);
        const body = (await res.json().catch(() => ({}))) as {
          step?: string;
          expected_signals?: string[];
        };
        if (!cancelled && res.ok) setHint(body);
      } catch {
        /* optional */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [base, live]);

  const runSeed = useCallback(async () => {
    if (!live) return;
    setSeedBusy(true);
    setErr(null);
    setSeedOut(null);
    setRecOut(null);
    try {
      const res = await fetch(`${base}/api/ledger/gas-town-crash-demo`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          application_id: appId.trim() || "APEX-GAS-DEMO",
          agent_id: agentId.trim() || "credit_analysis",
        }),
      });
      const body = (await res.json().catch(() => ({}))) as SeedResponse;
      if (!res.ok) {
        setErr(typeof body.detail === "string" ? body.detail : `${res.status}: ${JSON.stringify(body).slice(0, 320)}`);
        return;
      }
      setSeedOut(body);
      if (body.session_id) setSessionId(body.session_id);
    } catch (e) {
      setErr(String(e));
    } finally {
      setSeedBusy(false);
    }
  }, [base, live, appId, agentId]);

  const runReconstruct = useCallback(async () => {
    if (!live) return;
    const sid = sessionId.trim();
    const aid = agentId.trim() || "credit_analysis";
    if (!sid) {
      setErr("Set session_id (run seed first or paste from a prior run).");
      return;
    }
    setRecBusy(true);
    setErr(null);
    setRecOut(null);
    try {
      const res = await fetch(`${base}/api/ledger/reconstruct-agent-context`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ agent_id: aid, session_id: sid }),
      });
      const body = (await res.json().catch(() => ({}))) as ReconstructResponse;
      if (!res.ok) {
        setErr(typeof body.detail === "string" ? body.detail : `${res.status}: ${JSON.stringify(body).slice(0, 320)}`);
        return;
      }
      setRecOut(body);
    } catch (e) {
      setErr(String(e));
    } finally {
      setRecBusy(false);
    }
  }, [base, live, sessionId, agentId]);

  if (!live) {
    return (
      <div className="rounded-2xl border border-white/[0.06] bg-apex-slate/10 p-6 text-sm text-apex-mist">
        Connect the staff API for Gas Town demo endpoints.
      </div>
    );
  }

  return (
    <motion.div
      className="glass-panel rounded-2xl p-8"
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: [0.22, 1, 0.36, 1] }}
    >
      <h1 className="font-display text-3xl text-apex-ivory">Step 5 · Gas Town recovery</h1>
      <p className="mt-3 max-w-3xl text-sm text-apex-mist">
        Append a short agent session to <code className="font-mono text-apex-champagne">agent-{"{agent_id}"}-{"{session_id}"}</code>, then
        lose all process memory (stop <code className="text-apex-champagne">uvicorn</code>, or continue in a fresh tab with no local state).
        <code className="font-mono text-apex-champagne"> reconstruct_agent_context()</code> reloads the tail of the stream from Postgres so a
        worker can resume with the same logical position, pending reconciliation, and crash marker.
      </p>

      <ol className="mt-6 list-inside list-decimal space-y-2 text-sm text-apex-cream">
        <li>
          <strong className="text-apex-ivory">Seed</strong> — five events ending in <code className="text-apex-champagne">AgentSessionFailed</code>{" "}
          (persisted before &quot;death&quot;).
        </li>
        <li>
          <strong className="text-apex-ivory">Simulate crash</strong> — kill the API process or pretend the in-memory LangGraph runner vanished;
          the ledger row stays.
        </li>
        <li>
          <strong className="text-apex-ivory">Reconstruct</strong> — same <code className="text-apex-champagne">agent_id</code> +{" "}
          <code className="text-apex-champagne">session_id</code>; response includes <code className="text-apex-champagne">last_event_position</code>,{" "}
          <code className="text-apex-champagne">pending_work</code>, and verbatim tail lines in <code className="text-apex-champagne">context_text</code>.
        </li>
      </ol>

      {hint?.step ? (
        <aside className="mt-6 rounded-xl border border-apex-gold/20 bg-apex-gold/[0.06] p-4 text-xs text-apex-cream">
          <p className="font-semibold text-apex-ivory">Examiner narrative</p>
          <p className="mt-2 leading-relaxed text-apex-mist">{hint.step}</p>
          {hint.expected_signals?.length ? (
            <ul className="mt-3 list-inside list-disc space-y-1 text-[11px] text-apex-mist">
              {hint.expected_signals.map((s, i) => (
                <li key={i}>{s}</li>
              ))}
            </ul>
          ) : null}
        </aside>
      ) : null}

      <div className="mt-8 grid gap-4 sm:grid-cols-2">
        <label className="block text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">
          application_id (payload only)
          <input
            value={appId}
            onChange={(e) => setAppId(e.target.value)}
            className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-3 py-2 font-mono text-xs text-apex-ivory"
          />
        </label>
        <label className="block text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">
          agent_id
          <input
            value={agentId}
            onChange={(e) => setAgentId(e.target.value)}
            className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-3 py-2 font-mono text-xs text-apex-ivory"
          />
        </label>
        <label className="sm:col-span-2 block text-[10px] font-semibold uppercase tracking-wider text-apex-champagne">
          session_id (filled after seed)
          <input
            value={sessionId}
            onChange={(e) => setSessionId(e.target.value)}
            placeholder="sess-gas-…"
            className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-3 py-2 font-mono text-xs text-apex-ivory"
          />
        </label>
      </div>

      <div className="mt-6 flex flex-wrap gap-3">
        <button
          type="button"
          disabled={seedBusy}
          onClick={() => void runSeed()}
          className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-6 py-2.5 text-xs font-semibold text-apex-void shadow-lg disabled:opacity-40"
        >
          {seedBusy ? "Appending…" : "1 · Seed crash scenario"}
        </button>
        <button
          type="button"
          disabled={recBusy || !sessionId.trim()}
          onClick={() => void runReconstruct()}
          className="rounded-full border border-apex-gold/40 px-6 py-2.5 text-xs font-medium text-apex-gold hover:bg-apex-gold/10 disabled:opacity-40"
        >
          {recBusy ? "Replaying…" : "3 · Reconstruct from ledger"}
        </button>
        <button
          type="button"
          disabled={seedBusy || recBusy}
          onClick={() => {
            setSeedOut(null);
            setRecOut(null);
            setSessionId("");
            setErr(null);
          }}
          className="rounded-full border border-white/15 px-6 py-2.5 text-xs text-apex-mist hover:border-white/25 disabled:opacity-40"
        >
          Reset demo
        </button>
      </div>

      <p className="mt-4 text-[11px] text-apex-mist">
        Sample curl after seed:{" "}
        <code className="break-all font-mono text-apex-champagne/90">
          curl -sS -X POST {base}/api/ledger/reconstruct-agent-context -H &quot;Content-Type: application/json&quot; -d
          &apos;%7B&quot;agent_id&quot;:&quot;credit_analysis&quot;,&quot;session_id&quot;:&quot;YOUR_SESS&quot;%7D&apos;
        </code>
      </p>

      {err ? (
        <p className="mt-4 rounded-lg border border-rose-500/30 bg-rose-950/20 p-4 text-sm text-rose-100/90">{err}</p>
      ) : null}

      {seedOut?.ok ? (
        <div className="mt-6 rounded-xl border border-white/[0.08] bg-black/25 p-4">
          <p className="text-xs font-semibold text-apex-ivory">Seed result</p>
          <pre className="mt-2 max-h-48 overflow-auto text-[10px] text-apex-cream">{JSON.stringify(seedOut, null, 2)}</pre>
          {seedOut.narrative?.length ? (
            <ul className="mt-3 list-inside list-disc text-[11px] text-apex-mist">
              {seedOut.narrative.map((line, i) => (
                <li key={i}>{line}</li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}

      {recOut?.ok ? (
        <div className="mt-6 rounded-xl border border-emerald-500/20 bg-emerald-950/10 p-4">
          <p className="text-xs font-semibold text-emerald-100/90">Reconstruction (no in-memory session required)</p>
          <dl className="mt-3 grid gap-2 text-[11px] text-apex-cream sm:grid-cols-2">
            <div>
              <dt className="text-apex-mist">stream_id</dt>
              <dd className="break-all font-mono text-apex-champagne">{recOut.stream_id}</dd>
            </div>
            <div>
              <dt className="text-apex-mist">last_event_position</dt>
              <dd className="font-mono">{recOut.last_event_position}</dd>
            </div>
            <div>
              <dt className="text-apex-mist">persisted_event_count</dt>
              <dd className="font-mono">{recOut.persisted_event_count}</dd>
            </div>
            <div>
              <dt className="text-apex-mist">session_health_status</dt>
              <dd className="font-mono text-apex-gold">{recOut.session_health_status}</dd>
            </div>
            <div className="sm:col-span-2">
              <dt className="text-apex-mist">tail_event_types</dt>
              <dd className="font-mono">{recOut.tail_event_types?.join(" → ")}</dd>
            </div>
            <div className="sm:col-span-2">
              <dt className="text-apex-mist">pending_work</dt>
              <dd className="font-mono text-[10px]">{JSON.stringify(recOut.pending_work)}</dd>
            </div>
          </dl>
          <p className="mt-3 text-[10px] text-apex-mist">context_text</p>
          <pre className="mt-1 max-h-64 overflow-auto whitespace-pre-wrap rounded-lg border border-white/10 bg-black/40 p-3 text-[10px] text-apex-cream">
            {recOut.context_text}
          </pre>
        </div>
      ) : null}
    </motion.div>
  );
}
