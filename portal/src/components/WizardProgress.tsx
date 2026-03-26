import { motion } from "framer-motion";

const steps = [
  { id: 1, title: "Application", subtitle: "Core request" },
  { id: 2, title: "Documents", subtitle: "Financial package" },
  { id: 3, title: "Review", subtitle: "Confirm & submit" },
];

type Props = { current: number; className?: string };

export default function WizardProgress({ current, className = "" }: Props) {
  const pct = ((current - 1) / (steps.length - 1)) * 100;

  return (
    <div className={`space-y-8 ${className}`}>
      <div className="relative">
        <div className="absolute left-0 right-0 top-[22px] h-[2px] overflow-hidden rounded-full bg-white/[0.06]" />
        <motion.div
          className="absolute left-0 top-[22px] h-[2px] rounded-full gold-line origin-left"
          initial={{ scaleX: 0 }}
          animate={{ scaleX: pct / 100 }}
          transition={{ type: "spring", stiffness: 120, damping: 22, mass: 0.8 }}
          style={{ width: "100%" }}
        />
        <ul className="relative flex justify-between">
          {steps.map((s) => {
            const done = current > s.id;
            const active = current === s.id;
            return (
              <li key={s.id} className="flex flex-col items-center" style={{ width: "33.33%" }}>
                <motion.div
                  className={`relative z-10 flex h-11 w-11 items-center justify-center rounded-full border-2 text-sm font-semibold transition-colors ${
                    done
                      ? "border-apex-gold bg-apex-gold/20 text-apex-gold"
                      : active
                        ? "border-apex-gold bg-apex-night text-apex-gold shadow-[0_0_24px_rgba(212,175,55,0.25)]"
                        : "border-white/15 bg-apex-slate/50 text-apex-mist"
                  }`}
                  initial={false}
                  animate={{
                    scale: active ? 1.08 : 1,
                    boxShadow: active
                      ? "0 0 28px rgba(212,175,55,0.35)"
                      : "0 0 0 rgba(212,175,55,0)",
                  }}
                  transition={{ type: "spring", stiffness: 400, damping: 22 }}
                >
                  {done ? (
                    <motion.svg
                      className="h-5 w-5"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      initial={{ scale: 0, rotate: -45 }}
                      animate={{ scale: 1, rotate: 0 }}
                      transition={{ type: "spring", stiffness: 500, damping: 20 }}
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
                    </motion.svg>
                  ) : (
                    s.id
                  )}
                </motion.div>
                <motion.p
                  className={`mt-3 text-center font-display text-base ${
                    active || done ? "text-apex-ivory" : "text-apex-mist"
                  }`}
                  animate={{ opacity: active ? 1 : done ? 0.9 : 0.55 }}
                >
                  {s.title}
                </motion.p>
                <p className="text-center text-[11px] uppercase tracking-wider text-apex-mist/80">
                  {s.subtitle}
                </p>
              </li>
            );
          })}
        </ul>
      </div>

      <div className="overflow-hidden rounded-full bg-white/[0.04] p-[3px] ring-1 ring-white/[0.06]">
        <motion.div
          className="h-2 rounded-full bg-gradient-to-r from-apex-golddim via-apex-gold to-apex-champagne"
          initial={{ width: "0%" }}
          animate={{ width: `${(current / steps.length) * 100}%` }}
          transition={{ type: "spring", stiffness: 95, damping: 18 }}
          style={{
            boxShadow: "0 0 20px rgba(212,175,55,0.35)",
          }}
        />
      </div>
    </div>
  );
}
