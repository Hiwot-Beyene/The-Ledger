const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

export type ModelOverride = {
  provider: "ollama" | "openrouter" | "openai";
  model_name: string;
};

export type ProviderCatalog = {
  provider: "ollama" | "openrouter" | "openai";
  paid: boolean;
  requires_api_key: boolean;
  key_configured: boolean;
  models: string[];
};

export type ModelConfigResponse = {
  providers: ProviderCatalog[];
  discovery_errors?: Record<string, string>;
  default_policy: string;
  defaults: {
    provider: string;
    model: string;
    vision_provider: string;
    vision_model: string;
  };
  active: {
    auto_select: boolean;
    override?: ModelOverride;
    vision_override?: ModelOverride;
    summary_override?: ModelOverride;
    has_openrouter_api_key?: boolean;
    openrouter_base_url?: string | null;
    has_openai_api_key?: boolean;
    ollama_base_url?: string | null;
    has_ollama_api_key?: boolean;
    max_vision_budget_usd?: number;
    require_approval_over_budget?: boolean;
  };
};

export async function uploadDocument(file: File): Promise<{ doc_id: string }> {
  const formData = new FormData();
  formData.append("file", file);
  const response = await fetch(`${API_BASE}/documents/upload`, { method: "POST", body: formData });
  return response.json();
}

export async function listDocuments(): Promise<{ documents: Array<{ doc_id: string; document_name: string; status: string }> }> {
  const response = await fetch(`${API_BASE}/documents`);
  return response.json();
}

export async function deleteDocument(docId: string): Promise<{ ok: boolean; doc_id: string }> {
  const response = await fetch(`${API_BASE}/documents/${docId}`, { method: "DELETE" });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(typeof data?.detail === "string" ? data.detail : `Delete failed (${response.status})`);
  }
  return response.json();
}

export async function processDocument(
  docId: string,
  options?: { language_hint?: string; resume?: boolean; approve?: boolean }
): Promise<{ job_id: string; status: string; budget_status?: string; approval_required?: boolean; can_resume?: boolean }> {
  const body: { language_hint?: string; resume?: boolean; approve?: boolean } = {};
  if (options?.language_hint) body.language_hint = options.language_hint;
  if (options?.resume) body.resume = true;
  if (options?.approve) body.approve = true;
  const response = await fetch(`${API_BASE}/documents/${docId}/process`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return response.json();
}

export type JobStatus = {
  stage: string;
  status: string;
  progress_percent: number;
  error?: string;
  budget_status?: string;
  approval_required?: boolean;
  can_resume?: boolean;
  estimated_cost_usd?: number | null;
  budget_cap_usd?: number | null;
  page_count?: number;
  /** Actual cost after completion (from extraction ledger). */
  cost_estimate_usd?: number | null;
  processing_time_ms?: number | null;
  prompt_tokens?: number | null;
  completion_tokens?: number | null;
  /** Vision model pricing (when completed with token counts). */
  vision_input_per_1m_usd?: number | null;
  vision_output_per_1m_usd?: number | null;
  vision_model_name?: string | null;
};

export async function fetchJobStatus(docId: string): Promise<JobStatus> {
  const response = await fetch(`${API_BASE}/documents/${docId}/status`);
  return response.json();
}

export async function runQuery(payload: {
  doc_ids: string[];
  query: string;
  mode?: "answer" | "audit";
  model_override?: ModelOverride;
}): Promise<{ answer: string; provenance: Array<{ document_name: string; page_number: number; bbox: [number, number, number, number]; content_hash: string }> }> {
  const response = await fetch(`${API_BASE}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return response.json();
}

export async function saveModelConfig(config: { auto_select: boolean; override?: ModelOverride }) {
  const response = await fetch(`${API_BASE}/config/models`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  return response.json();
}

export async function getModelConfig(): Promise<ModelConfigResponse> {
  let response: Response;
  try {
    response = await fetch(`${API_BASE}/config/models`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Network error";
    throw new Error(`Cannot reach backend at ${API_BASE}. ${msg}`);
  }
  const data = await response.json();
  if (!response.ok) {
    throw new Error(typeof data?.detail === "string" ? data.detail : `Config failed (${response.status})`);
  }
  return data as ModelConfigResponse;
}

export async function saveModelConfigWithKeys(config: {
  auto_select: boolean;
  override?: ModelOverride;
  vision_override?: ModelOverride;
  summary_override?: ModelOverride;
  vision_source?: "local" | "cloud";
  openrouter_api_key?: string;
  openrouter_base_url?: string;
  openai_api_key?: string;
  max_vision_budget_usd?: number;
  require_approval_over_budget?: boolean;
}) {
  const response = await fetch(`${API_BASE}/config/models`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Save failed (${response.status})`);
  }
  return response.json();
}
