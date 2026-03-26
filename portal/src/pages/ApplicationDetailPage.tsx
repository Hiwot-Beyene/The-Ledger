import { useEffect, useState } from "react";
import { motion } from "framer-motion";
import { Link, useParams } from "react-router-dom";
import { getApiBase, useLedgerData } from "@/contexts/LedgerDataContext";
import { milestonesForApplication, type PipelineApplication } from "@/data/ledgerSeed";
import { formatConfidence, formatDateTime } from "@/lib/formatDateTime";
import { isLiveApiMode } from "@/lib/apiBase";

type DecisionPathStep = {
  step_id: string;
  label: string;
  description: string;
  status: string;
  recorded_at: string | null;
  event_type: string | null;
  outcome?: string | null;
  detail?: {
    recommendation?: string;
    confidence?: unknown;
    summary?: string | null;
    key_risks?: string[] | null;
    conditions?: string[] | null;
  };
  checks?: { event_type: string; label: string; completed: boolean; recorded_at: string | null }[];
};

type StreamEventRow = {
  event_type: string;
  stream_position: number;
  global_position: number;
  event_version: number;
  recorded_at: string | null;
  label: string;
};

type ApiSummary = {
  application_id: string;
  applicant_id: string;
  company_name: string;
  requested_amount_usd: string;
  loan_purpose: string;
  loan_term_months?: number | null;
  submission_channel?: string | null;
  submitted_at?: string | null;
  ui_stage: string;
  summary_projection_state: string | null | undefined;
  last_event_type: string | null;
  last_event_at?: string | null;
  binding_recorded_at?: string | null;
  stream_events?: StreamEventRow[];
  decision_path?: DecisionPathStep[];
  orchestrator_recommendation: {
    recommendation: string;
    confidence: number;
    summary?: string;
    at: string | null;
  } | null;
  human_review: {
    reviewer_id: string;
    original_recommendation: string;
    final_decision: string;
    override: boolean;
    decision_reason?: string | null;
    at: string | null;
    automated?: boolean;
  } | null;
  binding_outcome: "approved" | "declined" | null;
  milestones: { event_type: string; label: string; recorded_at: string | null }[];
  hitl_confidence_threshold?: number;
  hitl_required_by_policy?: boolean;
  human_review_requested?: boolean;
  staff_binding_required?: boolean;
};

function toneForEvent(ev: string): "gold" | "ivory" | "mist" {
  if (
    ev === "ApplicationSubmitted" ||
    ev === "DecisionGenerated" ||
    ev === "ApplicationApproved" ||
    ev === "ApplicationDeclined"
  ) {
    return "gold";
  }
  if (ev === "DocumentUploadRequested" || ev === "PackageReadyForAnalysis") return "mist";
  return "ivory";
}

function statusPillClass(status: string): string {
  switch (status) {
    case "completed":
      return "border-emerald-500/40 bg-emerald-950/30 text-emerald-100";
    case "in_progress":
    case "pending":
      return "border-amber-500/40 bg-amber-950/25 text-amber-100";
    case "skipped":
      return "border-sky-500/35 bg-sky-950/20 text-sky-100";
    case "blocked":
      return "border-white/15 bg-apex-slate/40 text-apex-mist";
    default:
      return "border-white/10 bg-apex-slate/30 text-apex-mist";
  }
}

export default function ApplicationDetailPage() {
  const rawId = useParams().id;
  const id = rawId?.trim() ?? "";
  const { pipeline } = useLedgerData();
  const app = id ? pipeline.find((p: PipelineApplication) => p.id === id) : undefined;
  const [apiSummary, setApiSummary] = useState<ApiSummary | null>(null);
  const [apiLoading, setApiLoading] = useState(false);
  const [apiErr, setApiErr] = useState<string | null>(null);
  const [humanBusy, setHumanBusy] = useState(false);
  const [reviewerId, setReviewerId] = useState("LO-Staff");
  const [decisionReason, setDecisionReason] = useState("");
  const [humanErr, setHumanErr] = useState<string | null>(null);

  const base = getApiBase();
  const live = isLiveApiMode();

  useEffect(() => {
    if (!live || !id) {
      setApiSummary(null);
      setApiErr(null);
      setApiLoading(false);
      return;
    }
    let cancel = false;
    let intervalId: number | undefined;
    let first = true;

    const fetchSummary = async () => {
      try {
        const res = await fetch(`${base}/api/applications/${encodeURIComponent(id)}/summary`);
        if (!res.ok) {
          if (!cancel) {
            setApiSummary(null);
            setApiErr(`${res.status}: ${await res.text()}`);
          }
          return;
        }
        const data = (await res.json()) as ApiSummary;
        if (!cancel) {
          setApiSummary(data);
          setApiErr(null);
          if (data.binding_outcome || data.ui_stage === "Complete") {
            if (intervalId) window.clearInterval(intervalId);
          }
        }
      } catch (e) {
        if (!cancel) {
          setApiSummary(null);
          setApiErr(String(e));
        }
      } finally {
        if (!cancel) {
          if (first) setApiLoading(false);
        }
        first = false;
      }
    };

    setApiLoading(true);
    setApiErr(null);
    intervalId = window.setInterval(() => {
      if (cancel) return;
      void fetchSummary();
    }, 2000);
    void fetchSummary();
    return () => {
      cancel = true;
      if (intervalId) window.clearInterval(intervalId);
    };
  }, [base, id, live]);

  const seedTimeline = id ? milestonesForApplication(id) : [];
  const apiStreamTimeline =
    apiSummary?.stream_events?.map((e) => ({
      event_type: e.event_type,
      label: e.label,
      recorded_at: e.recorded_at ?? "",
      tone: toneForEvent(e.event_type),
      meta: `#${e.stream_position} · g${e.global_position} · v${e.event_version}`,
    })) ?? [];
  const apiMilestoneTimeline =
    apiSummary?.milestones?.map((m) => ({
      event_type: m.event_type,
      label: m.label,
      recorded_at: m.recorded_at ?? "",
      tone: toneForEvent(m.event_type),
      meta: null as string | null,
    })) ?? [];

  const timeline =
    live && apiSummary?.stream_events?.length
      ? apiStreamTimeline
      : live && apiMilestoneTimeline.length > 0
        ? apiMilestoneTimeline
        : !live
          ? seedTimeline.map((m) => ({ ...m, meta: null as string | null }))
          : [];

  const title = live
    ? (apiSummary?.company_name ?? (apiLoading ? "Loading…" : "—"))
    : (apiSummary?.company_name ?? app?.companyName ?? "Application dossier");
  const amount = live
    ? (apiSummary?.requested_amount_usd ?? (apiLoading ? "…" : "—"))
    : (apiSummary?.requested_amount_usd ?? app?.amount ?? "—");
  const purpose = live
    ? (apiSummary?.loan_purpose ?? (apiLoading ? "…" : "—"))
    : (apiSummary?.loan_purpose ?? app?.purpose ?? "—");
  const stage = live
    ? (apiSummary?.ui_stage ?? (apiLoading ? "…" : "—"))
    : (apiSummary?.ui_stage ?? app?.state ?? "—");
  const applicantId = live ? apiSummary?.applicant_id : (apiSummary?.applicant_id ?? app?.applicantId);

  if (!id) {
    return (
      <div>
        <Link to="/" className="text-sm text-apex-mist hover:text-apex-champagne">
          ← Command
        </Link>
        <p className="mt-8 text-sm text-apex-mist">Missing application id in the URL.</p>
      </div>
    );
  }

  const orch = apiSummary?.orchestrator_recommendation;
  const showOrch = orch && orch.recommendation != null && String(orch.recommendation).length > 0;
  const hr = apiSummary?.human_review;
  const showHr =
    hr &&
    hr.reviewer_id != null &&
    String(hr.reviewer_id).length > 0 &&
    hr.automated !== true;
  const staffBindingRequired = apiSummary?.staff_binding_required !== false;

  const submitHumanDecision = async (finalDecision: "APPROVE" | "DECLINE") => {
    if (!live || !apiSummary?.orchestrator_recommendation?.recommendation) return;
    const original = String(apiSummary.orchestrator_recommendation.recommendation).toUpperCase();
    const override = original !== finalDecision;
    if (!decisionReason.trim()) {
      setHumanErr("Brief rationale is required for approve or decline.");
      return;
    }
    setHumanErr(null);
    setHumanBusy(true);
    try {
      const res = await fetch(`${base}/api/applications/${encodeURIComponent(id)}/human-review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          reviewer_id: reviewerId.trim() || "LO-Staff",
          override,
          original_recommendation: original,
          final_decision: finalDecision,
          decision_reason: decisionReason.trim(),
        }),
      });
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        setHumanErr(`${res.status}: ${txt || "request failed"}`);
        return;
      }
    } catch (e) {
      setHumanErr(String(e));
    } finally {
      setHumanBusy(false);
    }
  };

  return (
    <div>
      <Link to="/" className="text-sm text-apex-mist hover:text-apex-champagne">
        ← Command
      </Link>

      {live && apiErr ? (
        <p className="mt-6 rounded-xl border border-rose-500/30 bg-rose-950/20 p-4 text-sm text-rose-100/90">
          Could not load dossier from the database: {apiErr}
        </p>
      ) : null}

      {live && apiLoading ? (
        <p className="mt-6 text-sm text-apex-mist">Loading application dossier from Postgres…</p>
      ) : null}

      <div className="mt-8 flex flex-col gap-8 lg:flex-row lg:items-start">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="glass-panel flex-1 space-y-6 rounded-2xl p-8 lg:max-w-md"
        >
          <div>
            <p className="font-mono text-sm text-apex-gold">{id}</p>
            <h1 className="mt-2 font-display text-3xl text-apex-ivory">{title}</h1>
            {applicantId ? <p className="mt-2 text-sm text-apex-mist">{applicantId}</p> : null}
            <div className="mt-8 space-y-4 border-t border-white/[0.06] pt-8 text-sm">
              <div className="flex justify-between">
                <span className="text-apex-mist">Amount</span>
                <span className="text-apex-cream">{amount}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-apex-mist">Purpose</span>
                <span className="text-apex-cream">{purpose}</span>
              </div>
              {apiSummary?.loan_term_months != null ? (
                <div className="flex justify-between">
                  <span className="text-apex-mist">Term (months)</span>
                  <span className="text-apex-cream">{String(apiSummary.loan_term_months)}</span>
                </div>
              ) : null}
              {apiSummary?.submission_channel ? (
                <div className="flex justify-between">
                  <span className="text-apex-mist">Channel</span>
                  <span className="text-apex-cream">{apiSummary.submission_channel}</span>
                </div>
              ) : null}
              <div className="flex justify-between">
                <span className="text-apex-mist">Stage</span>
                <span className="rounded-full bg-apex-gold/15 px-3 py-0.5 text-xs text-apex-gold">{stage}</span>
              </div>
              {live && apiSummary?.submitted_at ? (
                <div className="flex justify-between">
                  <span className="text-apex-mist">Submitted</span>
                  <span className="text-apex-cream">{formatDateTime(apiSummary.submitted_at)}</span>
                </div>
              ) : null}
              {live && apiSummary?.last_event_at ? (
                <div className="flex justify-between">
                  <span className="text-apex-mist">Last stream event</span>
                  <span className="text-apex-cream">{formatDateTime(apiSummary.last_event_at)}</span>
                </div>
              ) : null}
            </div>
          </div>

          {live && apiSummary?.decision_path?.length ? (
            <div className="rounded-xl border border-white/[0.08] bg-apex-slate/15 p-4">
              <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-apex-champagne">Decision path (from loan stream)</p>
              <ol className="mt-4 space-y-4">
                {apiSummary.decision_path.map((step, i) => (
                  <li key={step.step_id} className="border-l border-white/10 pl-4">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-xs font-mono text-apex-mist/80">{i + 1}.</span>
                      <span className={`rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-wider ${statusPillClass(step.status)}`}>
                        {step.status.replace("_", " ")}
                      </span>
                    </div>
                    <p className="mt-1 font-medium text-apex-ivory">{step.label}</p>
                    <p className="mt-0.5 text-xs text-apex-mist">{step.description}</p>
                    {step.recorded_at ? (
                      <p className="mt-1 text-[11px] text-apex-champagne/90">{formatDateTime(step.recorded_at)}</p>
                    ) : null}
                    {step.event_type ? (
                      <p className="mt-0.5 font-mono text-[10px] text-apex-mist/70">{step.event_type}</p>
                    ) : null}
                    {step.outcome ? (
                      <p className="mt-1 text-xs text-apex-cream">
                        Outcome: <span className="font-medium capitalize">{step.outcome}</span>
                      </p>
                    ) : null}
                    {step.checks?.length ? (
                      <ul className="mt-2 space-y-1 text-xs text-apex-mist">
                        {step.checks.map((c) => (
                          <li key={c.event_type}>
                            {c.completed ? "✓" : "○"} {c.label}
                            {c.recorded_at ? ` · ${formatDateTime(c.recorded_at)}` : ""}
                          </li>
                        ))}
                      </ul>
                    ) : null}
                    {step.detail?.recommendation != null && step.detail.recommendation !== "" ? (
                      <p className="mt-2 text-xs text-apex-cream">
                        Model: {String(step.detail.recommendation)} · confidence {formatConfidence(step.detail.confidence)}
                      </p>
                    ) : null}
                    {step.detail?.summary ? (
                      <p className="mt-2 text-xs leading-relaxed text-apex-mist">
                        {step.detail.summary}
                      </p>
                    ) : null}
                    {step.detail?.key_risks?.length ? (
                      <ul className="mt-2 space-y-1 text-xs text-apex-mist">
                        {step.detail.key_risks.map((r, idx) => (
                          <li key={`${r}-${idx}`}>• {r}</li>
                        ))}
                      </ul>
                    ) : null}
                  </li>
                ))}
              </ol>
            </div>
          ) : null}

          {showOrch ? (
            <div className="rounded-xl border border-white/[0.08] bg-apex-slate/20 p-4">
              <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-apex-champagne">
                AI recommendation (not binding)
              </p>
              <p className="mt-2 font-display text-lg text-apex-ivory">{String(orch!.recommendation)}</p>
              <p className="mt-1 text-xs text-apex-mist">
                Confidence {formatConfidence(orch!.confidence)}
                {orch!.at ? ` · ${formatDateTime(orch!.at)}` : ""}
              </p>
              {orch!.summary ? (
                <p className="mt-3 text-xs leading-relaxed text-apex-mist">{orch!.summary}</p>
              ) : null}
            </div>
          ) : null}

          {live && showOrch && !apiSummary?.binding_outcome && staffBindingRequired ? (
            <div className="mt-4 rounded-xl border border-apex-gold/20 bg-apex-gold/5 p-4">
              <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-apex-champagne">
                Human decision (binding)
              </p>
              {apiSummary?.human_review_requested ? (
                <p className="mt-2 text-xs text-apex-mist">
                  <span className="font-mono text-apex-cream">HumanReviewRequested</span> is on the stream — staff binding is required when confidence is below{" "}
                  {apiSummary.hitl_confidence_threshold ?? 0.6} or when{" "}
                  <span className="font-mono">LEDGER_AUTO_BIND_HIGH_CONFIDENCE</span> is off.
                </p>
              ) : null}
              <p className="mt-2 text-sm text-apex-mist">
                Model recommendation:{" "}
                <span className="text-apex-ivory">{String(orch!.recommendation)}</span>
              </p>

              <div className="mt-4 space-y-3">
                <div className="flex items-center justify-between gap-3">
                  <p className="text-xs text-apex-mist">Reviewer</p>
                  <input
                    value={reviewerId}
                    onChange={(e) => setReviewerId(e.target.value)}
                    className="w-[220px] rounded-lg border border-white/10 bg-apex-slate/40 px-3 py-2 text-xs text-apex-ivory"
                    placeholder="human reviewer id"
                  />
                </div>

                <div className="flex gap-2">
                  <button
                    type="button"
                    disabled={humanBusy}
                    onClick={() => void submitHumanDecision("APPROVE")}
                    className="flex-1 rounded-full bg-emerald-500/15 px-4 py-2 text-xs font-medium text-emerald-100 disabled:opacity-40"
                  >
                    Approve
                  </button>
                  <button
                    type="button"
                    disabled={humanBusy}
                    onClick={() => void submitHumanDecision("DECLINE")}
                    className="flex-1 rounded-full bg-rose-500/15 px-4 py-2 text-xs font-medium text-rose-100 disabled:opacity-40"
                  >
                    Decline
                  </button>
                </div>

                <div>
                  <p className="text-[10px] font-semibold uppercase tracking-wider text-apex-mist">
                    Reason for decision (required)
                  </p>
                  <textarea
                    value={decisionReason}
                    onChange={(e) => setDecisionReason(e.target.value)}
                    rows={3}
                    className="mt-2 w-full resize-y rounded-lg border border-white/10 bg-apex-slate/40 px-3 py-2 text-xs text-apex-ivory"
                    placeholder="Why approve or decline (auditable; include policy or facts cited)."
                  />
                </div>

                {humanErr ? (
                  <p className="text-xs text-rose-100/90">
                    {humanErr}
                  </p>
                ) : null}
              </div>
            </div>
          ) : null}

          {showHr ? (
            <div className="rounded-xl border border-apex-gold/20 bg-apex-gold/5 p-4">
              <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-apex-champagne">Human in the loop</p>
              <p className="mt-2 text-sm text-apex-cream">
                Reviewer <span className="text-apex-ivory">{String(hr!.reviewer_id)}</span>
              </p>
              <p className="mt-2 text-xs text-apex-mist">
                Original model recommendation:{" "}
                <span className="text-apex-ivory">{hr!.original_recommendation != null ? String(hr!.original_recommendation) : "—"}</span>
              </p>
              <p className="mt-1 text-xs text-apex-mist">
                Recorded decision:{" "}
                <span className="font-medium text-apex-ivory">
                  {hr!.final_decision != null ? String(hr!.final_decision) : "—"}
                </span>
                {hr!.override ? " (override)" : ""}
              </p>
              {hr!.decision_reason ? (
                <p className="mt-2 text-xs leading-relaxed text-apex-cream">
                  <span className="text-apex-mist">Staff rationale: </span>
                  {String(hr!.decision_reason)}
                </p>
              ) : null}
              {hr!.at ? <p className="mt-1 text-[11px] text-apex-champagne/90">{formatDateTime(hr!.at)}</p> : null}
            </div>
          ) : null}

          {apiSummary?.binding_outcome ? (
            <div
              className={`rounded-xl border p-4 text-sm ${
                apiSummary.binding_outcome === "approved"
                  ? "border-emerald-500/30 bg-emerald-950/20 text-emerald-100/90"
                  : "border-rose-500/30 bg-rose-950/20 text-rose-100/90"
              }`}
            >
              <p className="text-[10px] font-semibold uppercase tracking-[0.2em] opacity-80">Binding outcome</p>
              <p className="mt-2 font-medium capitalize">{apiSummary.binding_outcome}</p>
              <p className="mt-1 text-xs opacity-80">
                Recorded as Application{apiSummary.binding_outcome === "approved" ? "Approved" : "Declined"} on the loan stream.
              </p>
              {apiSummary.binding_recorded_at ? (
                <p className="mt-2 text-[11px] opacity-90">{formatDateTime(apiSummary.binding_recorded_at)}</p>
              ) : null}
              {apiSummary.human_review?.decision_reason ? (
                <p className="mt-3 text-xs leading-relaxed opacity-90">
                  <span className="font-medium opacity-100">Binding rationale: </span>
                  {String(apiSummary.human_review.decision_reason)}
                </p>
              ) : null}
            </div>
          ) : live && apiSummary && !apiSummary.binding_outcome ? (
            <p className="text-xs text-apex-mist">
              {staffBindingRequired
                ? "No binding yet — complete human review above when ready."
                : "Binding runs automatically when confidence is at/above the floor and auto-bind is enabled (including high-confidence REFER); refresh if the workflow just finished."}
            </p>
          ) : null}

          <p className="text-xs leading-relaxed text-apex-mist">
            {live && apiSummary
              ? apiSummary.summary_projection_state == null
                ? "All fields above are read from Postgres (`events` + registry). Projection table optional."
                : "Dossier backed by `events` and `application_summary` projection."
              : live && apiErr
                ? "Fix API or database, then refresh. Offline seed narrative is not mixed into live mode."
                : live
                  ? "Staff API on :8080 (Vite proxies `/api`)."
                  : "Production build: set VITE_API_URL for live data."}
          </p>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, x: 24 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.12 }}
          className="glass-panel flex-[2] rounded-2xl p-8"
        >
          <h2 className="font-display text-xl text-apex-ivory">Event narrative</h2>
          <p className="mt-1 text-xs text-apex-mist">
            {live && apiSummary?.stream_events?.length
              ? `Full loan stream from database (${apiSummary.stream_events.length} events, chronological)`
              : live && apiSummary
                ? "Milestone snapshot (latest event per type)"
                : !live
                  ? "Milestones from seed_events.jsonl"
                  : apiLoading
                    ? "…"
                    : "No stream loaded"}
          </p>
          <ul className="relative mt-10 space-y-0">
            <div className="absolute left-[15px] top-2 bottom-2 w-px bg-gradient-to-b from-apex-gold/50 via-white/10 to-transparent" />
            {timeline.length === 0 ? (
              <p className="text-sm text-apex-mist">
                {live && !apiLoading && !apiErr ? "No events for this application in the database." : "No milestones to show."}
              </p>
            ) : (
              timeline.map((ev, i) => (
                <motion.li
                  key={`${ev.event_type}-${ev.recorded_at}-${i}`}
                  initial={{ opacity: 0, x: 16 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.08 * i }}
                  className="relative flex gap-6 pb-10 last:pb-0"
                >
                  <motion.div
                    className={`relative z-10 mt-0.5 h-8 w-8 shrink-0 rounded-full border-2 ${
                      ev.tone === "gold"
                        ? "border-apex-gold bg-apex-gold/20 shadow-[0_0_16px_rgba(212,175,55,0.3)]"
                        : "border-white/20 bg-apex-slate"
                    }`}
                    whileInView={{ scale: [1, 1.08, 1] }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.5 }}
                  />
                  <div>
                    <p className="font-medium text-apex-ivory">{ev.label}</p>
                    <p className="mt-0.5 font-mono text-[11px] text-apex-mist">{ev.event_type}</p>
                    {"meta" in ev && ev.meta ? (
                      <p className="mt-0.5 font-mono text-[10px] text-apex-mist/70">{ev.meta}</p>
                    ) : null}
                    <p className="mt-1 text-[11px] text-apex-champagne/90">{formatDateTime(ev.recorded_at)}</p>
                  </div>
                </motion.li>
              ))
            )}
          </ul>
        </motion.div>
      </div>

    </div>
  );
}
