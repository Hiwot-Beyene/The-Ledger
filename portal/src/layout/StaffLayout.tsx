import { motion, AnimatePresence } from "framer-motion";
import { Link, Outlet, useLocation } from "react-router-dom";

const nav = [
  { to: "/", label: "Command" },
  { to: "/applications/new", label: "New intake" },
  { to: "/pipeline", label: "Pipeline" },
  { to: "/navigator", label: "Navigator" },
];

export default function StaffLayout() {
  const location = useLocation();

  return (
    <div className="min-h-screen flex flex-col">
      <header className="sticky top-0 z-50 border-b border-white/[0.06] bg-apex-night/80 backdrop-blur-xl">
        <div className="mx-auto flex h-16 max-w-7xl items-center justify-between px-6 lg:px-10">
          <Link to="/" className="group flex items-center gap-3">
            <motion.div
              className="flex h-10 w-10 items-center justify-center rounded-lg border border-apex-gold/30 bg-gradient-to-br from-apex-gold/20 to-transparent"
              whileHover={{ scale: 1.05, borderColor: "rgba(212,175,55,0.5)" }}
              transition={{ type: "spring", stiffness: 400, damping: 25 }}
            >
              <span className="font-display text-lg font-semibold text-apex-gold">A</span>
            </motion.div>
            <div>
              <p className="font-display text-lg font-semibold tracking-wide text-apex-ivory">
                Apex Ledger
              </p>
              <p className="text-[10px] uppercase tracking-[0.25em] text-apex-mist">Staff portal</p>
            </div>
          </Link>
          <nav className="hidden items-center gap-1 md:flex">
            {nav.map((item) => {
              const active =
                item.to === "/"
                  ? location.pathname === "/"
                  : location.pathname.startsWith(item.to);
              return (
                <Link key={item.to} to={item.to}>
                  <motion.span
                    className={`relative block rounded-full px-4 py-2 text-sm font-medium transition-colors ${
                      active ? "text-apex-ivory" : "text-apex-mist hover:text-apex-cream"
                    }`}
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                  >
                    {active && (
                      <motion.span
                        layoutId="nav-pill"
                        className="absolute inset-0 rounded-full bg-white/[0.08] ring-1 ring-apex-gold/20"
                        transition={{ type: "spring", stiffness: 380, damping: 30 }}
                      />
                    )}
                    <span className="relative z-10">{item.label}</span>
                  </motion.span>
                </Link>
              );
            })}
          </nav>
          <div className="flex items-center gap-3">
            <motion.div
              className="hidden h-8 w-px bg-white/10 sm:block"
              initial={{ scaleY: 0 }}
              animate={{ scaleY: 1 }}
            />
            <div className="text-right">
              <p className="text-xs font-medium text-apex-ivory">Hiwot Beyene</p>
              <p className="text-[10px] text-apex-mist">Signed in</p>
            </div>
            <motion.div
              className="relative h-9 w-9 overflow-hidden rounded-full border border-apex-gold/25 bg-gradient-to-br from-apex-slate to-apex-night"
              whileHover={{ scale: 1.06 }}
            >
              <img
                src="/me.jpg"
                alt="Hiwot Beyene profile"
                className="h-full w-full rounded-full object-cover opacity-95"
              />
            </motion.div>
          </div>
        </div>
      </header>

      <main className="relative flex-1">
        <AnimatePresence mode="wait">
          <motion.div
            key={location.pathname}
            initial={{ opacity: 0, y: 16, filter: "blur(6px)" }}
            animate={{ opacity: 1, y: 0, filter: "blur(0px)" }}
            exit={{ opacity: 0, y: -12, filter: "blur(4px)" }}
            transition={{ duration: 0.42, ease: [0.22, 1, 0.36, 1] }}
            className="mx-auto max-w-7xl px-6 py-10 lg:px-10"
          >
            <Outlet />
          </motion.div>
        </AnimatePresence>
      </main>

      <footer className="border-t border-white/[0.04] py-6 text-center text-[11px] text-apex-mist">
        Event-sourced loan decisions · Immutable audit trail · Internal use only
      </footer>
    </div>
  );
}
