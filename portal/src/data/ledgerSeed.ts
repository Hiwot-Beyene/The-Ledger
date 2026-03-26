import applicantProfilesJson from "@repo-data/applicant_profiles.json";
import seedEventsRaw from "@repo-data/seed_events.jsonl?raw";

export type ApplicantProfile = {
  company_id: string;
  name: string;
  industry: string;
  jurisdiction: string;
  legal_type: string;
  trajectory: string;
  risk_segment: string;
  compliance_flags: unknown[];
};

export const APPLICANT_PROFILES: ApplicantProfile[] = applicantProfilesJson as ApplicantProfile[];

export const LOAN_PURPOSES = [
  "working_capital",
  "equipment_financing",
  "real_estate",
  "expansion",
  "refinancing",
  "acquisition",
  "bridge",
] as const;

export type LoanPurposeValue = (typeof LOAN_PURPOSES)[number];

export type PipelineStage = "Intake" | "Documents" | "Credit" | "Fraud" | "Compliance" | "Decision" | "Complete";

export type PipelineApplication = {
  id: string;
  applicantId: string;
  companyName: string;
  amount: string;
  purpose: string;
  state: PipelineStage;
  updated: string;
  lastEventAt: string;
};

type SeedRow = {
  stream_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  recorded_at: string;
};

const MILESTONE_TYPES = new Set([
  "ApplicationSubmitted",
  "DocumentUploadRequested",
  "DocumentUploaded",
  "PackageCreated",
  "PackageReadyForAnalysis",
  "CreditAnalysisRequested",
  "CreditAnalysisCompleted",
  "FraudScreeningRequested",
  "FraudScreeningCompleted",
  "ComplianceCheckRequested",
  "ComplianceCheckCompleted",
  "DecisionRequested",
  "DecisionGenerated",
  "HumanReviewRequested",
  "HumanReviewCompleted",
  "ApplicationApproved",
  "ApplicationDeclined",
]);

function extractApplicationId(row: SeedRow): string | null {
  const aid = row.payload.application_id;
  if (typeof aid === "string" && /^APEX-\d+$/.test(aid)) return aid;
  const m = row.stream_id.match(/^(?:loan|credit|fraud|compliance|docpkg)-(APEX-\d+)$/);
  return m?.[1] ?? null;
}

function milestoneToStage(eventType: string): PipelineStage {
  switch (eventType) {
    case "ApplicationApproved":
    case "ApplicationDeclined":
    case "HumanReviewCompleted":
      return "Complete";
    case "HumanReviewRequested":
    case "DecisionGenerated":
    case "DecisionRequested":
      return "Decision";
    case "ComplianceCheckCompleted":
    case "ComplianceCheckRequested":
      return "Compliance";
    case "FraudScreeningCompleted":
    case "FraudScreeningRequested":
      return "Fraud";
    case "CreditAnalysisCompleted":
    case "CreditAnalysisRequested":
      return "Credit";
    case "PackageReadyForAnalysis":
    case "PackageCreated":
    case "DocumentUploaded":
      return "Documents";
    default:
      return "Intake";
  }
}

function formatUsd(value: unknown): string {
  const n = typeof value === "string" ? parseFloat(value) : typeof value === "number" ? value : NaN;
  if (!Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(n);
}

function formatPurpose(raw: unknown): string {
  if (typeof raw !== "string" || !raw) return "—";
  return raw.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatUpdated(iso: string): string {
  const d = Date.parse(iso);
  if (!Number.isFinite(d)) return "—";
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }).format(
    new Date(d),
  );
}

const companyById = new Map(APPLICANT_PROFILES.map((c) => [c.company_id, c]));

function parseSeedRows(raw: string): SeedRow[] {
  const out: SeedRow[] = [];
  for (const line of raw.split("\n")) {
    if (!line.trim()) continue;
    try {
      out.push(JSON.parse(line) as SeedRow);
    } catch {
      /* skip */
    }
  }
  return out;
}

function buildPipelineApplications(rows: SeedRow[]): PipelineApplication[] {
  const aggs = new Map<
    string,
    { lastT: string; lastType: string; submitted: Record<string, unknown> | null }
  >();

  for (const row of rows) {
    const appId = extractApplicationId(row);
    if (!appId) continue;

    let a = aggs.get(appId) ?? { lastT: "", lastType: "ApplicationSubmitted", submitted: null };
    if (row.event_type === "ApplicationSubmitted") {
      a.submitted = row.payload;
    }
    if (MILESTONE_TYPES.has(row.event_type)) {
      if (!a.lastT || row.recorded_at >= a.lastT) {
        a.lastT = row.recorded_at;
        a.lastType = row.event_type;
      }
    }
    aggs.set(appId, a);
  }

  const apps: PipelineApplication[] = [];
  for (const [id, a] of aggs) {
    const sub = a.submitted;
    const applicantId = typeof sub?.applicant_id === "string" ? sub.applicant_id : "—";
    const co = companyById.get(applicantId);
    apps.push({
      id,
      applicantId,
      companyName: co?.name ?? applicantId,
      amount: formatUsd(sub?.requested_amount_usd),
      purpose: formatPurpose(sub?.loan_purpose),
      state: milestoneToStage(a.lastType),
      updated: formatUpdated(a.lastT),
      lastEventAt: a.lastT,
    });
  }

  apps.sort((x, y) => x.id.localeCompare(y.id));
  return apps;
}

const seedRows = parseSeedRows(seedEventsRaw);

export const PIPELINE_APPLICATIONS: PipelineApplication[] = buildPipelineApplications(seedRows);

const DETAIL_TYPES = [
  "ApplicationSubmitted",
  "DocumentUploadRequested",
  "PackageReadyForAnalysis",
  "CreditAnalysisCompleted",
  "FraudScreeningCompleted",
  "ComplianceCheckCompleted",
  "DecisionGenerated",
  "HumanReviewCompleted",
  "ApplicationApproved",
  "ApplicationDeclined",
] as const;

const EVENT_LABELS: Record<string, string> = {
  ApplicationSubmitted: "Application captured",
  DocumentUploadRequested: "Documents requested",
  PackageReadyForAnalysis: "Document package ready for analysis",
  CreditAnalysisCompleted: "Credit analysis",
  FraudScreeningCompleted: "Fraud screening",
  ComplianceCheckCompleted: "Compliance verdict",
  DecisionGenerated: "Orchestrator decision",
  HumanReviewCompleted: "Human review",
  ApplicationApproved: "Final approval",
  ApplicationDeclined: "Application declined",
};

export type DetailMilestone = { event_type: string; label: string; recorded_at: string; tone: "gold" | "ivory" | "mist" };

export function milestonesForApplication(applicationId: string | undefined): DetailMilestone[] {
  if (!applicationId) return [];
  const wanted = new Set<string>(DETAIL_TYPES);
  const latestByType = new Map<string, string>();
  for (const row of seedRows) {
    if (extractApplicationId(row) !== applicationId) continue;
    if (!wanted.has(row.event_type)) continue;
    const prev = latestByType.get(row.event_type);
    if (!prev || row.recorded_at >= prev) latestByType.set(row.event_type, row.recorded_at);
  }
  const dedup = [...latestByType.entries()].map(([event_type, recorded_at]) => ({ event_type, recorded_at }));
  dedup.sort((a, b) => a.recorded_at.localeCompare(b.recorded_at));
  return dedup.map((h) => ({
    event_type: h.event_type,
    label: EVENT_LABELS[h.event_type] ?? h.event_type,
    recorded_at: h.recorded_at,
    tone:
      h.event_type === "ApplicationSubmitted" ||
      h.event_type === "DecisionGenerated" ||
      h.event_type === "ApplicationApproved" ||
      h.event_type === "ApplicationDeclined"
        ? "gold"
        : h.event_type === "DocumentUploadRequested" || h.event_type === "PackageReadyForAnalysis"
          ? "mist"
          : "ivory",
  }));
}
