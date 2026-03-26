import { useState, useCallback, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Link } from "react-router-dom";
import WizardProgress from "@/components/WizardProgress";
import { useLedgerData } from "@/contexts/LedgerDataContext";
import { LOAN_PURPOSES } from "@/data/ledgerSeed";
import { submitApplication, uploadApplicationDocuments } from "@/lib/api";

const fadeSlide = {
  initial: { opacity: 0, x: 40, filter: "blur(8px)" },
  animate: { opacity: 1, x: 0, filter: "blur(0px)" },
  exit: { opacity: 0, x: -32, filter: "blur(6px)" },
};

function nextId() {
  const n = Math.floor(1000 + Math.random() * 8999);
  return `APEX-${n}`;
}

export default function NewApplicationPage() {
  const { applicants, refetch, source, loading: registryLoading, error: registryError } = useLedgerData();
  const [step, setStep] = useState(1);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [doneId, setDoneId] = useState<string | null>(null);

  const [applicationId, setApplicationId] = useState(nextId);
  const [applicantId, setApplicantId] = useState("");
  const [amount, setAmount] = useState("500000");
  const [purpose, setPurpose] = useState<string>(LOAN_PURPOSES[0]);
  const [termMonths, setTermMonths] = useState("36");
  const [contactName, setContactName] = useState("");
  const [contactEmail, setContactEmail] = useState("");
  const [channel, setChannel] = useState<"branch" | "web">("branch");

  const [files, setFiles] = useState<{
    proposal: File | null;
    income: File | null;
    balance: File | null;
  }>({ proposal: null, income: null, balance: null });

  useEffect(() => {
    if (!applicants.length) return;
    if (!applicantId || !applicants.some((a) => a.company_id === applicantId)) {
      setApplicantId(applicants[0]!.company_id);
    }
  }, [applicants, applicantId]);

  const applicant = applicants.find((a) => a.company_id === applicantId);

  const canNext1 = applicantId && Number(amount) > 0 && purpose;
  const canNext2 = files.proposal && files.income && files.balance;

  const goNext = useCallback(() => {
    setStep((s) => Math.min(3, s + 1));
  }, []);

  const goBack = useCallback(() => {
    setStep((s) => Math.max(1, s - 1));
  }, []);

  const handleSubmit = async () => {
    setSubmitting(true);
    setSubmitError(null);
    const res = await submitApplication({
      application_id: applicationId,
      applicant_id: applicantId,
      requested_amount_usd: Number(amount),
      loan_purpose: purpose,
      loan_term_months: Number(termMonths) || undefined,
      submission_channel: channel,
      contact_name: contactName || undefined,
      contact_email: contactEmail || undefined,
    });
    setSubmitting(false);
    if (res.ok) {
      if (source === "api") {
        if (!files.proposal || !files.income || !files.balance) {
          setSubmitError("Upload proposal, income, and balance documents before committing.");
          return;
        }
        const ures = await uploadApplicationDocuments(applicationId, {
          proposal: files.proposal,
          income: files.income,
          balance: files.balance,
        });
        if (!ures.ok) {
          setSubmitError(ures.error ?? "Document upload failed");
          return;
        }
      }
      setDoneId(applicationId);
      void refetch();
    } else {
      setSubmitError(res.error ?? "Submit failed");
    }
  };

  if (doneId) {
    return (
      <motion.div
        className="flex min-h-[70vh] flex-col items-center justify-center text-center"
        initial={{ opacity: 0, scale: 0.92 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ type: "spring", stiffness: 200, damping: 22 }}
      >
        <motion.div
          className="mb-8 flex h-24 w-24 items-center justify-center rounded-full border border-apex-gold/40 bg-apex-gold/10"
          initial={{ scale: 0 }}
          animate={{ scale: 1, rotate: [0, 5, -5, 0] }}
          transition={{ delay: 0.15, type: "spring", stiffness: 260, damping: 16 }}
        >
          <svg className="h-12 w-12 text-apex-gold" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M5 13l4 4L19 7" />
          </svg>
        </motion.div>
        <h1 className="font-display text-4xl font-semibold text-apex-ivory md:text-5xl">Intake recorded</h1>
        <p className="mt-4 max-w-md text-apex-mist">
          Application <span className="text-apex-gold">{doneId}</span> is on the ledger with{" "}
          <code className="text-apex-champagne/90">DocumentUploadRequested</code>. Open the dossier to see stage,
          decision path, and full stream (document processing will start automatically).
        </p>
        <div className="mt-10 flex flex-wrap justify-center gap-4">
          <Link
            to={`/applications/${doneId}`}
            className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-8 py-3 text-sm font-semibold text-apex-void shadow-lg shadow-apex-gold/20"
          >
            View dossier
          </Link>
          <button
            type="button"
            onClick={() => {
              setDoneId(null);
              setStep(1);
              setApplicationId(nextId());
              setFiles({ proposal: null, income: null, balance: null });
            }}
            className="rounded-full border border-white/15 px-8 py-3 text-sm font-medium text-apex-cream hover:border-apex-gold/40"
          >
            Another intake
          </button>
        </div>
      </motion.div>
    );
  }

  return (
    <div className="pb-16">
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
        className="mb-12 text-center"
      >
        <p className="text-xs uppercase tracking-[0.35em] text-apex-champagne">New application</p>
        <h1 className="mt-3 font-display text-4xl font-semibold tracking-tight text-apex-ivory md:text-5xl">
          Institutional intake
        </h1>
        <p className="mx-auto mt-4 max-w-xl text-sm text-apex-mist">
          Capture the structured application, then bind the financial document package. Each advance commits
          progress to the ledger narrative.
        </p>
        <p className="mx-auto mt-2 max-w-xl text-xs text-apex-mist/80">
          Data:{" "}
          <span className="text-apex-champagne/90">
            {source === "api" ? "applicant_registry (Postgres via API)" : "embedded applicant_profiles.json"}
          </span>
          {source === "seed" && (
            <>
              {" "}
              · run <code className="text-apex-champagne/80">npm run dev</code> + staff API on :8080 (or set{" "}
              <code className="text-apex-champagne/80">VITE_API_URL</code>) for Postgres
            </>
          )}
        </p>
        {registryError && source === "api" ? (
          <p className="mx-auto mt-3 max-w-xl rounded-xl border border-amber-500/30 bg-amber-950/20 px-4 py-2 text-xs text-amber-100/90">
            Registry failed to load — not using JSON fallback. {registryError}
          </p>
        ) : null}
      </motion.div>

      <div className="glass-panel mx-auto max-w-3xl rounded-2xl p-8 md:p-12">
        <WizardProgress current={step} className="mb-14" />

        <div className="relative min-h-[420px]">
          <AnimatePresence mode="wait">
            {step === 1 && (
              <motion.div
                key="s1"
                {...fadeSlide}
                transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
                className="space-y-8"
              >
                {registryLoading && !applicants.length ? (
                  <p className="text-center text-sm text-apex-mist">Loading registry…</p>
                ) : null}
                <div className="grid gap-6 md:grid-cols-2">
                  <div className="md:col-span-2">
                    <label className="block text-xs uppercase tracking-wider text-apex-mist">Application ID</label>
                    <motion.input
                      value={applicationId}
                      onChange={(e) => setApplicationId(e.target.value)}
                      className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-4 py-3 font-mono text-sm text-apex-ivory outline-none ring-apex-gold/30 focus:border-apex-gold/40 focus:ring-2"
                      whileFocus={{ scale: 1.01 }}
                      transition={{ type: "spring", stiffness: 400, damping: 30 }}
                    />
                  </div>
                  <div className="md:col-span-2">
                    <label className="block text-xs uppercase tracking-wider text-apex-mist">Applicant (company)</label>
                    <select
                      value={applicantId}
                      onChange={(e) => setApplicantId(e.target.value)}
                      className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-4 py-3 text-sm text-apex-ivory outline-none focus:border-apex-gold/40 focus:ring-2 focus:ring-apex-gold/20"
                    >
                      {applicants.map((a) => (
                        <option key={a.company_id} value={a.company_id}>
                          {a.name} · {a.company_id}
                        </option>
                      ))}
                    </select>
                    {applicant && (
                      <motion.p
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: "auto" }}
                        className="mt-2 text-xs text-apex-mist"
                      >
                        {applicant.industry} · {applicant.jurisdiction} · {applicant.legal_type} · Risk{" "}
                        {applicant.risk_segment}
                      </motion.p>
                    )}
                  </div>
                  <div>
                    <label className="block text-xs uppercase tracking-wider text-apex-mist">
                      Requested amount (USD)
                    </label>
                    <input
                      type="number"
                      min={1}
                      value={amount}
                      onChange={(e) => setAmount(e.target.value)}
                      className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-4 py-3 text-sm text-apex-ivory outline-none focus:border-apex-gold/40 focus:ring-2 focus:ring-apex-gold/20"
                    />
                  </div>
                  <div>
                    <label className="block text-xs uppercase tracking-wider text-apex-mist">Loan purpose</label>
                    <select
                      value={purpose}
                      onChange={(e) => setPurpose(e.target.value)}
                      className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-4 py-3 text-sm text-apex-ivory outline-none focus:border-apex-gold/40 focus:ring-2 focus:ring-apex-gold/20"
                    >
                      {LOAN_PURPOSES.map((p) => (
                        <option key={p} value={p}>
                          {p.replace(/_/g, " ")}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs uppercase tracking-wider text-apex-mist">Term (months)</label>
                    <input
                      type="number"
                      min={1}
                      value={termMonths}
                      onChange={(e) => setTermMonths(e.target.value)}
                      className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-4 py-3 text-sm text-apex-ivory outline-none focus:border-apex-gold/40 focus:ring-2 focus:ring-apex-gold/20"
                    />
                  </div>
                  <div>
                    <label className="block text-xs uppercase tracking-wider text-apex-mist">Channel</label>
                    <div className="mt-2 flex gap-2">
                      {(["branch", "web"] as const).map((c) => (
                        <button
                          key={c}
                          type="button"
                          onClick={() => setChannel(c)}
                          className={`flex-1 rounded-xl border py-3 text-sm capitalize transition-colors ${
                            channel === c
                              ? "border-apex-gold/50 bg-apex-gold/10 text-apex-gold"
                              : "border-white/10 text-apex-mist hover:border-white/20"
                          }`}
                        >
                          {c}
                        </button>
                      ))}
                    </div>
                  </div>
                  <div>
                    <label className="block text-xs uppercase tracking-wider text-apex-mist">Contact name</label>
                    <input
                      value={contactName}
                      onChange={(e) => setContactName(e.target.value)}
                      placeholder="Officer of record"
                      className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-4 py-3 text-sm text-apex-ivory placeholder:text-apex-mist/50 outline-none focus:border-apex-gold/40 focus:ring-2 focus:ring-apex-gold/20"
                    />
                  </div>
                  <div>
                    <label className="block text-xs uppercase tracking-wider text-apex-mist">Contact email</label>
                    <input
                      type="email"
                      value={contactEmail}
                      onChange={(e) => setContactEmail(e.target.value)}
                      placeholder="name@apex.com"
                      className="mt-2 w-full rounded-xl border border-white/10 bg-apex-slate/40 px-4 py-3 text-sm text-apex-ivory placeholder:text-apex-mist/50 outline-none focus:border-apex-gold/40 focus:ring-2 focus:ring-apex-gold/20"
                    />
                  </div>
                </div>
              </motion.div>
            )}

            {step === 2 && (
              <motion.div
                key="s2"
                {...fadeSlide}
                transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
                className="space-y-6"
              >
                <p className="text-sm text-apex-mist">
                  Upload the three required artifacts. Names should mirror your{" "}
                  <code className="rounded bg-white/5 px-1.5 py-0.5 text-apex-champagne">documents/</code> bundle.
                </p>
                <div className="grid gap-5">
                  {(
                    [
                      { key: "proposal" as const, label: "Application proposal", hint: "application_proposal.pdf" },
                      { key: "income" as const, label: "Income statement", hint: "income_statement_2024.pdf" },
                      { key: "balance" as const, label: "Balance sheet", hint: "balance_sheet_2024.pdf" },
                    ] as const
                  ).map((item, i) => (
                    <motion.label
                      key={item.key}
                      initial={{ opacity: 0, y: 24 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: i * 0.08, duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
                      className="group relative block cursor-pointer overflow-hidden rounded-2xl border border-dashed border-white/15 bg-apex-slate/20 p-8 transition-colors hover:border-apex-gold/35 hover:bg-apex-slate/35"
                      whileHover={{ scale: 1.01 }}
                      whileTap={{ scale: 0.995 }}
                    >
                      <input
                        type="file"
                        accept=".pdf,.xlsx,.csv"
                        className="hidden"
                        onChange={(e) => {
                          const f = e.target.files?.[0] ?? null;
                          setFiles((prev) => ({ ...prev, [item.key]: f }));
                        }}
                      />
                      <div className="flex items-center justify-between gap-4">
                        <div>
                          <p className="font-display text-lg text-apex-ivory">{item.label}</p>
                          <p className="mt-1 text-xs text-apex-mist">{item.hint}</p>
                        </div>
                        <motion.div
                          className={`flex h-14 w-14 shrink-0 items-center justify-center rounded-xl border ${
                            files[item.key]
                              ? "border-apex-gold/50 bg-apex-gold/15 text-apex-gold"
                              : "border-white/10 text-apex-mist"
                          }`}
                          animate={{ rotate: files[item.key] ? 0 : 0 }}
                        >
                          {files[item.key] ? (
                            <motion.span initial={{ scale: 0 }} animate={{ scale: 1 }} className="text-2xl">
                              ✓
                            </motion.span>
                          ) : (
                            <span className="text-xl opacity-60">↑</span>
                          )}
                        </motion.div>
                      </div>
                      {files[item.key] && (
                        <motion.p
                          initial={{ opacity: 0 }}
                          animate={{ opacity: 1 }}
                          className="mt-4 truncate text-xs text-apex-champagne"
                        >
                          {files[item.key]!.name}
                        </motion.p>
                      )}
                      <motion.div
                        className="pointer-events-none absolute inset-0 bg-gold-shine bg-[length:200%_100%] opacity-0 transition-opacity group-hover:opacity-100"
                        style={{ backgroundPosition: "200% 0" }}
                      />
                    </motion.label>
                  ))}
                </div>
              </motion.div>
            )}

            {step === 3 && (
              <motion.div
                key="s3"
                {...fadeSlide}
                transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
                className="space-y-8"
              >
                <div className="rounded-2xl border border-white/[0.08] bg-black/20 p-6">
                  <h3 className="font-display text-xl text-apex-ivory">Submission manifest</h3>
                  <dl className="mt-6 space-y-4 text-sm">
                    {[
                      ["Application", applicationId],
                      ["Applicant", `${applicant?.name ?? "—"} (${applicantId})`],
                      ["Amount", `$${Number(amount).toLocaleString()}`],
                      ["Purpose", purpose.replace(/_/g, " ")],
                      ["Term", `${termMonths} mo`],
                      ["Channel", channel],
                      ["Documents", `${[files.proposal, files.income, files.balance].filter(Boolean).length} / 3`],
                    ].map(([k, v]) => (
                      <div key={k} className="flex justify-between gap-4 border-b border-white/[0.04] pb-3 last:border-0">
                        <dt className="text-apex-mist">{k}</dt>
                        <dd className="text-right text-apex-ivory">{v}</dd>
                      </div>
                    ))}
                  </dl>
                </div>
                <p className="text-xs text-apex-mist">
                  Commits <code className="text-apex-champagne">ApplicationSubmitted</code> to the ledger. Requires an existing{" "}
                  <code className="text-apex-champagne">applicant_id</code> in the registry when using the API.
                </p>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {submitError && step === 3 ? (
          <p className="mt-6 rounded-xl border border-red-500/30 bg-red-950/20 px-4 py-3 text-sm text-red-200/90">
            {submitError}
          </p>
        ) : null}

        <div className="mt-12 flex flex-col-reverse gap-4 border-t border-white/[0.06] pt-10 sm:flex-row sm:justify-between">
          <div className="flex gap-3">
            {step > 1 && (
              <motion.button
                type="button"
                onClick={goBack}
                className="rounded-full border border-white/15 px-6 py-3 text-sm font-medium text-apex-cream hover:border-white/25"
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
              >
                Back
              </motion.button>
            )}
            <Link
              to="/"
              className="rounded-full px-6 py-3 text-sm text-apex-mist hover:text-apex-cream"
            >
              Cancel
            </Link>
          </div>
          <div className="flex gap-3">
            {step < 3 ? (
              <motion.button
                type="button"
                disabled={step === 1 ? !canNext1 : !canNext2}
                onClick={goNext}
                className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-10 py-3 text-sm font-semibold text-apex-void shadow-lg shadow-apex-gold/25 disabled:cursor-not-allowed disabled:opacity-40"
                whileHover={{ scale: step === 1 ? (canNext1 ? 1.03 : 1) : canNext2 ? 1.03 : 1 }}
                whileTap={{ scale: 0.97 }}
                transition={{ type: "spring", stiffness: 400, damping: 22 }}
              >
                Continue
              </motion.button>
            ) : (
              <motion.button
                type="button"
                disabled={submitting}
                onClick={handleSubmit}
                className="rounded-full bg-gradient-to-r from-apex-gold to-apex-champagne px-10 py-3 text-sm font-semibold text-apex-void shadow-lg shadow-apex-gold/25 disabled:opacity-50"
                whileHover={{ scale: 1.03 }}
                whileTap={{ scale: 0.97 }}
              >
                {submitting ? "Committing…" : "Commit to ledger"}
              </motion.button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
