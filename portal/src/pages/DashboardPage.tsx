import { motion } from "framer-motion";
import { Link } from "react-router-dom";
import { useLedgerData } from "@/contexts/LedgerDataContext";
import Spline from "@splinetool/react-spline";

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.08, delayChildren: 0.1 },
  },
};

const item = {
  hidden: { opacity: 0, y: 28 },
  show: { opacity: 1, y: 0, transition: { type: "spring", stiffness: 300, damping: 28 } },
};

export default function DashboardPage() {
  const { pipeline, source, error } = useLedgerData();
  return (
    <div>
      <section className="relative overflow-hidden rounded-3xl border border-white/[0.07] bg-lux-gradient px-8 py-16 md:px-16 md:py-24">
        <motion.div
          className="pointer-events-none absolute -right-20 top-0 h-96 w-96 rounded-full bg-apex-gold/10 blur-[100px]"
          animate={{ scale: [1, 1.1, 1], opacity: [0.3, 0.5, 0.3] }}
          transition={{ duration: 8, repeat: Infinity, ease: "easeInOut" }}
        />
        <div className="relative z-10 grid gap-10 lg:grid-cols-[1fr_460px] lg:items-center">
          <div className="max-w-2xl">
            <motion.p
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 }}
              className="text-xs uppercase tracking-[0.4em] text-apex-champagne"
            >
              Apex Financial Services
            </motion.p>
            <motion.h1
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2, duration: 0.6 }}
              className="mt-4 font-display text-4xl font-semibold leading-tight text-apex-ivory md:text-6xl md:leading-[1.1]"
            >
              Command center for
              <span className="block bg-gradient-to-r from-apex-ivory via-apex-champagne to-apex-gold bg-clip-text text-transparent">
                event-sourced credit
              </span>
            </motion.h1>
            <motion.p
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.45 }}
              className="mt-6 max-w-lg text-sm leading-relaxed text-apex-mist"
            >
              Every intake, document, and decision becomes an immutable fact on the ledger. Orchestrate applications
              with the same rigor as a trading floor.
            </motion.p>
            <motion.div
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.55 }}
              className="mt-10 flex flex-wrap gap-4"
            >
              <Link to="/applications/new">
                <motion.span
                  className="inline-flex items-center rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-8 py-3.5 text-sm font-semibold text-apex-void shadow-xl shadow-apex-gold/20"
                  whileHover={{ scale: 1.04, boxShadow: "0 20px 40px rgba(212,175,55,0.25)" }}
                  whileTap={{ scale: 0.98 }}
                >
                  Begin new intake
                </motion.span>
              </Link>
              <Link to="/pipeline">
                <motion.span
                  className="inline-flex items-center rounded-full border border-white/20 px-8 py-3.5 text-sm font-medium text-apex-cream hover:border-apex-gold/40"
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                >
                  View pipeline
                </motion.span>
              </Link>
            </motion.div>
          </div>

          <div className="relative h-[360px] w-full overflow-hidden rounded-3xl border border-white/[0.07] bg-black/20 lg:h-[420px]">
            <Spline
              scene="https://prod.spline.design/uswUYWK4tJBopfX1/scene.splinecode"
              style={{ position: "absolute", inset: 0 }}
            />
          </div>
        </div>
      </section>

      <section className="mt-16">
        <div className="mb-8 flex items-end justify-between">
          <div>
            <h2 className="font-display text-2xl text-apex-ivory">Active matters</h2>
            <p className="mt-1 text-sm text-apex-mist">
              {pipeline.length} application{pipeline.length === 1 ? "" : "s"} ·{" "}
              <span className="text-apex-champagne/90">
                {source === "api" ? "application_summary projection (Postgres)" : "embedded seed_events.jsonl"}
              </span>
              {error && source === "api" ? (
                <span className="block text-xs text-amber-200/80">API error (empty list): {error}</span>
              ) : null}
            </p>
          </div>
          <Link to="/pipeline" className="text-sm text-apex-champagne hover:text-apex-gold">
            All →
          </Link>
        </div>

        <motion.ul
          variants={container}
          initial="hidden"
          animate="show"
          className="grid gap-5 md:grid-cols-2 lg:grid-cols-3"
        >
          {pipeline.map((app) => (
            <motion.li key={app.id} variants={item}>
              <Link to={`/applications/${app.id}`}>
                <motion.div
                  className="glass-panel group h-full rounded-2xl p-6 transition-colors hover:border-apex-gold/20"
                  whileHover={{ y: -4 }}
                  transition={{ type: "spring", stiffness: 400, damping: 25 }}
                >
                  <div className="flex items-start justify-between">
                    <span className="font-mono text-sm text-apex-gold">{app.id}</span>
                    <span className="rounded-full bg-white/5 px-2.5 py-0.5 text-[10px] uppercase tracking-wider text-apex-mist">
                      {app.state}
                    </span>
                  </div>
                  <p className="mt-4 font-display text-xl text-apex-ivory group-hover:text-white">{app.companyName}</p>
                  <p className="mt-1 text-xs text-apex-mist">{app.applicantId}</p>
                  <div className="mt-6 flex items-center justify-between border-t border-white/[0.06] pt-4 text-sm">
                    <span className="font-medium text-apex-cream">{app.amount}</span>
                    <span className="text-apex-mist">{app.updated}</span>
                  </div>
                </motion.div>
              </Link>
            </motion.li>
          ))}
        </motion.ul>
      </section>
    </div>
  );
}
