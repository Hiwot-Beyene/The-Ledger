import { isLiveApiMode, resolveApiBase } from "./apiBase";

export type SubmitApplicationBody = {
  application_id: string;
  applicant_id: string;
  requested_amount_usd: number;
  loan_purpose?: string;
  loan_term_months?: number;
  submission_channel?: string;
  contact_name?: string;
  contact_email?: string;
};

export async function submitApplication(body: SubmitApplicationBody): Promise<{ ok: boolean; error?: string }> {
  if (!isLiveApiMode()) {
    await new Promise((r) => setTimeout(r, 600));
    return { ok: true };
  }
  const base = resolveApiBase();
  try {
    const res = await fetch(`${base}/api/applications`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const raw = await res.text();
      try {
        const j = JSON.parse(raw) as { detail?: string | string[] };
        const d = j.detail;
        const msg = typeof d === "string" ? d : Array.isArray(d) ? d.join(", ") : raw;
        return { ok: false, error: msg };
      } catch {
        return { ok: false, error: raw };
      }
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

export async function uploadApplicationDocuments(
  application_id: string,
  files: { proposal: File; income: File; balance: File },
): Promise<{ ok: boolean; error?: string }> {
  if (!isLiveApiMode()) return { ok: true };

  const base = resolveApiBase();
  const fd = new FormData();
  fd.append("proposal", files.proposal);
  fd.append("income", files.income);
  fd.append("balance", files.balance);

  try {
    const res = await fetch(`${base}/api/applications/${encodeURIComponent(application_id)}/documents?auto_run=true`, {
      method: "POST",
      body: fd,
    });
    if (!res.ok) {
      const raw = await res.text();
      return { ok: false, error: raw };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}
