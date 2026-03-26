import { useEffect, useMemo, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Link } from "react-router-dom";
import { useLedgerData } from "@/contexts/LedgerDataContext";
import type { PipelineStage } from "@/data/ledgerSeed";

const STAGES: PipelineStage[] = [
  "Intake",
  "Documents",
  "Credit",
  "Fraud",
  "Compliance",
  "Decision",
  "Complete",
];

export default function PipelinePage() {
  const { pipeline, source, error } = useLedgerData();
  const [active, setActive] = useState<PipelineStage>("Intake");

  const byStage = useMemo(() => {
    const m = new Map<PipelineStage, typeof pipeline>();
    for (const s of STAGES) m.set(s, []);
    for (const p of pipeline) {
      const list = m.get(p.state);
      if (list) list.push(p);
    }
    for (const s of STAGES) {
      m.get(s)!.sort((a, b) => b.lastEventAt.localeCompare(a.lastEventAt));
    }
    return m;
  }, [pipeline]);

  useEffect(() => {
    if (!pipeline.length) return;
    setActive((cur) => {
      if ((byStage.get(cur) ?? []).length > 0) return cur;
      return STAGES.find((s) => (byStage.get(s) ?? []).length > 0) ?? cur;
    });
  }, [pipeline, byStage]);

  const items = byStage.get(active) ?? [];
  const activeIndex = STAGES.indexOf(active);

  return (
    <div>
      <h1 className="font-display text-3xl text-apex-ivory md:text-4xl">Pipeline</h1>
      <p className="mt-2 max-w-2xl text-sm text-apex-mist">
        One tab per lifecycle stage. Data:{" "}
        <span className="text-apex-champagne/90">
          {source === "api" ? "application_summary + events (API)" : "embedded seed_events.jsonl"}
        </span>
        .
      </p>
      {error && source === "api" ? (
        <p className="mt-2 text-xs text-amber-200/80">API error — {error}</p>
      ) : null}

      <div className="mt-10 glass-panel rounded-2xl p-2 md:p-3">
        <div
          role="tablist"
          aria-label="Pipeline stages"
          className="flex gap-1 overflow-x-auto border-b border-white/[0.07] pb-2"
        >
          {STAGES.map((stage) => {
            const count = byStage.get(stage)?.length ?? 0;
            const isActive = active === stage;
            return (
              <button
                key={stage}
                type="button"
                role="tab"
                id={`pipeline-tab-${stage}`}
                aria-selected={isActive}
                aria-controls={`pipeline-panel-${stage}`}
                tabIndex={isActive ? 0 : -1}
                onClick={() => setActive(stage)}
                className={`relative shrink-0 rounded-xl px-4 py-2.5 text-left transition-colors ${
                  isActive
                    ? "text-apex-ivory"
                    : "text-apex-mist hover:bg-white/[0.04] hover:text-apex-cream"
                }`}
              >
                {isActive && (
                  <motion.span
                    layoutId="pipeline-tab-bg"
                    className="absolute inset-0 rounded-xl bg-apex-gold/12 ring-1 ring-apex-gold/25"
                    transition={{ type: "spring", stiffness: 400, damping: 35 }}
                  />
                )}
                <span className="relative flex items-center gap-2">
                  <span className="whitespace-nowrap text-xs font-semibold uppercase tracking-[0.18em]">{stage}</span>
                  <span
                    className={`min-w-[1.25rem] rounded-full px-1.5 py-0.5 text-center text-[10px] font-medium ${
                      isActive ? "bg-apex-gold/25 text-apex-champagne" : "bg-white/10 text-apex-mist"
                    }`}
                  >
                    {count}
                  </span>
                </span>
              </button>
            );
          })}
        </div>

        <div
          role="tabpanel"
          id={`pipeline-panel-${active}`}
          aria-labelledby={`pipeline-tab-${active}`}
          className="min-h-[320px] p-4 md:p-6"
        >
          <AnimatePresence mode="wait">
            <motion.div
              key={active}
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -8 }}
              transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            >
              <div className="mb-6 flex items-end justify-between gap-4">
                <div>
                  <p className="text-xs uppercase tracking-[0.25em] text-apex-champagne">Stage {activeIndex + 1} of 7</p>
                  <h2 className="mt-1 font-display text-2xl text-apex-ivory">{active}</h2>
                </div>
                <p className="text-sm text-apex-mist">
                  {items.length} application{items.length === 1 ? "" : "s"}
                </p>
              </div>

              {items.length === 0 ? (
                <p className="rounded-xl border border-dashed border-white/10 py-16 text-center text-sm text-apex-mist/70">
                  No applications in this stage.
                </p>
              ) : (
                <ul className="space-y-3">
                  {items.map((app, i) => (
                    <motion.li
                      key={app.id}
                      initial={{ opacity: 0, x: -12 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: Math.min(i * 0.04, 0.4), duration: 0.3 }}
                    >
                      <Link
                        to={`/applications/${app.id}`}
                        className="group block rounded-xl border border-white/[0.08] bg-apex-slate/20 p-4 transition-colors hover:border-apex-gold/30 hover:bg-apex-slate/35 md:p-5"
                      >
                        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                          <div>
                            <p className="font-mono text-xs text-apex-gold">{app.id}</p>
                            <p className="mt-1 font-display text-xl text-apex-ivory group-hover:text-white">
                              {app.companyName}
                            </p>
                            <p className="mt-0.5 text-xs text-apex-mist">{app.applicantId}</p>
                          </div>
                          <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm md:text-right">
                            <div>
                              <p className="text-[10px] uppercase tracking-wider text-apex-mist">Amount</p>
                              <p className="font-medium text-apex-cream">{app.amount}</p>
                            </div>
                            <div>
                              <p className="text-[10px] uppercase tracking-wider text-apex-mist">Purpose</p>
                              <p className="text-apex-cream">{app.purpose}</p>
                            </div>
                            <div>
                              <p className="text-[10px] uppercase tracking-wider text-apex-mist">Updated</p>
                              <p className="text-apex-mist">{app.updated}</p>
                            </div>
                          </div>
                        </div>
                      </Link>
                    </motion.li>
                  ))}
                </ul>
              )}
            </motion.div>
          </AnimatePresence>
        </div>
      </div>
    </div>
  );
}
