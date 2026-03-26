"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { ChatPanel } from "../components/chat-panel";
import { DocumentList, type DocumentListItem } from "../components/document-list";
import { DocumentUploader } from "../components/document-uploader";
import { ModelConfigPanel, type ModelConfig, type ProviderCatalog } from "../components/model-config-panel";
import { getModelConfig, listDocuments, processDocument, runQuery, saveModelConfigWithKeys, uploadDocument, deleteDocument } from "../lib/api-client";
import { useJobStatus } from "../lib/use-job-status";

function formatElapsed(seconds: number): string {
  if (seconds < 60) return `0:${seconds.toString().padStart(2, "0")}`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}m ${s}s`;
}

export default function HomePage() {
  const [documents, setDocuments] = useState<DocumentListItem[]>([]);
  const [selectedDocId, setSelectedDocId] = useState<string | null>(null);
  const [modelConfig, setModelConfig] = useState<ModelConfig>({
    auto_select: false,
    query_provider: "ollama",
    query_model_name: "",
    vision_provider: "ollama",
    vision_model_name: "llava:7b",
    vision_source: "local",
    summary_provider: "ollama",
    summary_model_name: "",
    max_vision_budget_usd: Number(process.env.NEXT_PUBLIC_DEFAULT_VISION_BUDGET_USD ?? "2.0"),
    require_approval_over_budget: false,
  });
  const [providerCatalog, setProviderCatalog] = useState<ProviderCatalog[]>([]);
  const [discoveryErrors, setDiscoveryErrors] = useState<Record<string, string>>({});
  const [configError, setConfigError] = useState<string | null>(null);
  const [languageHint, setLanguageHint] = useState<string>("");

  const loadModelConfig = async () => {
    setConfigError(null);
    try {
      const payload = await getModelConfig();
    setProviderCatalog(payload.providers as ProviderCatalog[]);
    setDiscoveryErrors(payload.discovery_errors ?? {});

    const def = payload.defaults ?? {};
    const qOverride = payload.active?.override;
    const vOverride = payload.active?.vision_override;
    const visionSource = (payload.active?.vision_source ?? "local") as "local" | "cloud";
    const qProvider = "ollama";
    const vProvider = "ollama";
    const sProvider = "ollama";
    const qEntry = payload.providers.find((e: { provider: string }) => e.provider === qProvider);
    const vEntry = payload.providers.find((e: { provider: string }) => e.provider === vProvider);
    const sOverride = payload.active?.summary_override;
    const sEntry = payload.providers.find((e: { provider: string }) => e.provider === sProvider);
    const qModels = (qEntry?.models ?? []) as string[];
    const vModels = (vEntry?.models ?? []) as string[];

    const resolveModel = (overrideName: string | undefined, defaultName: string, list: string[]) =>
      overrideName && list.includes(overrideName) ? overrideName : (list.includes(defaultName) ? defaultName : list[0] ?? defaultName);

    const qModel = resolveModel(qOverride?.model_name, def.model ?? "", qModels);
    const vModel = visionSource === "cloud" ? "qwen3-vl:235b-instruct-cloud" : resolveModel(vOverride?.model_name, def.vision_model ?? "llava:7b", vModels);
    const sModels = (sEntry?.models ?? []) as string[];
    const sModel = resolveModel(sOverride?.model_name, def.model ?? "", sModels.length ? sModels : qModels);

    setModelConfig((prev) => ({
      ...prev,
      auto_select: payload.active?.auto_select ?? false,
      query_provider: qProvider,
      query_model_name: qModel,
      vision_provider: vProvider,
      vision_model_name: vModel,
      vision_source: visionSource,
      summary_provider: sProvider,
      summary_model_name: sModel,
      openrouter_base_url: payload.active?.openrouter_base_url ?? undefined,
      max_vision_budget_usd: payload.active?.max_vision_budget_usd ?? prev.max_vision_budget_usd,
      require_approval_over_budget: payload.active?.require_approval_over_budget ?? prev.require_approval_over_budget,
    }));
    } catch (err) {
      setConfigError(err instanceof Error ? err.message : "Failed to load model config");
    }
  };

  useEffect(() => {
    void loadModelConfig();
  }, []);

  useEffect(() => {
    listDocuments()
      .then((r) => setDocuments(r.documents))
      .catch(() => setDocuments((prev) => prev));
  }, []);

  const status = useJobStatus(selectedDocId, 1500);

  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const startTimeRef = useRef<number | null>(null);

  useEffect(() => {
    if (!selectedDocId || !status) {
      startTimeRef.current = null;
      setElapsedSeconds(0);
      return;
    }
                if (status.status === "running") {
      if (startTimeRef.current === null) startTimeRef.current = Date.now();
      const interval = window.setInterval(() => {
        const start = startTimeRef.current;
        if (start) setElapsedSeconds(Math.floor((Date.now() - start) / 1000));
      }, 1000);
      return () => window.clearInterval(interval);
    }
    if (status.status === "completed" || status.status === "failed") {
      const start = startTimeRef.current;
      if (start) setElapsedSeconds(Math.floor((Date.now() - start) / 1000));
      startTimeRef.current = null;
    } else {
      startTimeRef.current = null;
      setElapsedSeconds(0);
    }
  }, [selectedDocId, status?.status]);

  const selectedDocIds = useMemo(() => (selectedDocId ? [selectedDocId] : []), [selectedDocId]);

  useEffect(() => {
    if (status?.status === "completed" || status?.status === "failed" || status?.status === "approval_required") {
      listDocuments().then((r) => setDocuments(r.documents));
    }
  }, [status?.status]);

  return (
    <main className="flex min-h-0 flex-1 flex-col">
      <div className="mx-auto w-full max-w-[1520px] flex-1 px-4 py-5 sm:px-6 lg:px-8">
        <div className="grid min-h-[calc(100vh-11rem)] flex-1 gap-5 md:grid-cols-[340px_1fr] lg:grid-cols-[360px_300px_minmax(0,1fr)]">
          <aside className="panel-elevated flex flex-col overflow-hidden lg:max-h-[calc(100vh-11rem)]">
            <div className="flex flex-col gap-1 border-b border-slate-100 px-5 py-4">
              <span className="pill w-fit">Settings</span>
              <h2 className="font-display text-base font-semibold text-slate-900">Settings</h2>
            </div>
            {configError ? (
              <div className="border-b border-slate-100 px-5 py-4">
                <div className="rounded-lg border border-amber-200 bg-amber-50/90 px-3 py-2 text-xs text-amber-900">
                  {configError} Start the backend and refresh.
                </div>
              </div>
            ) : null}
            <div className="flex-1 overflow-y-auto px-5 py-4">
              <ModelConfigPanel
                initial={modelConfig}
                providers={providerCatalog}
                discoveryErrors={discoveryErrors}
                onRefresh={loadModelConfig}
                onSave={async (config) => {
                  setModelConfig(config);
                  await saveModelConfigWithKeys({
                    auto_select: config.auto_select,
                    override: config.auto_select ? undefined : { provider: "ollama", model_name: config.query_model_name },
                    vision_override: config.vision_source === "cloud" ? undefined : { provider: "ollama", model_name: config.vision_model_name },
                    summary_override: { provider: "ollama", model_name: config.summary_model_name },
                    vision_source: config.vision_source,
                    openrouter_api_key: config.openrouter_api_key,
                    openrouter_base_url: config.openrouter_base_url,
                    openai_api_key: config.openai_api_key,
                    max_vision_budget_usd: config.max_vision_budget_usd,
                  });
                  await loadModelConfig();
                }}
              />
            </div>
          </aside>

          <section className="panel-elevated flex flex-col overflow-hidden lg:max-h-[calc(100vh-11rem)]">
            <div className="flex flex-col gap-1 border-b border-slate-100 px-5 py-4">
              <span className="pill w-fit">Library</span>
              <h2 className="font-display text-base font-semibold text-slate-900">Documents</h2>
              <p className="text-xs text-slate-500">Upload PDFs and select a document to query or monitor.</p>
              <div className="mt-2 flex items-center gap-2">
                <label htmlFor="language-hint" className="text-xs font-medium text-slate-600">Language (optional)</label>
                <select
                  id="language-hint"
                  value={languageHint}
                  onChange={(e) => setLanguageHint(e.target.value)}
                  className="rounded border border-slate-200 bg-white px-2 py-1.5 text-xs text-slate-800 focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400"
                >
                  <option value="">Auto (detect from document)</option>
                  <option value="en">English</option>
                  <option value="ar">Arabic</option>
                  <option value="ti">Tigrinya</option>
                  <option value="am">Amharic</option>
                </select>
              </div>
            </div>
            <div className="flex-1 overflow-y-auto px-5 py-4">
              <DocumentUploader
                onUpload={async (file) => {
                  const { doc_id } = await uploadDocument(file);
                  setDocuments((prev) => [...prev, { doc_id, document_name: file.name, status: "uploaded" }]);
                  setSelectedDocId(doc_id);
                  try {
                    await processDocument(doc_id, languageHint ? { language_hint: languageHint } : undefined);
                  } catch {
                    // process started in background; status will update via polling
                  }
                  const next = await listDocuments();
                  setDocuments(next.documents);
                }}
              />
              <DocumentList
                documents={documents}
                selectedDocId={selectedDocId}
                onSelect={setSelectedDocId}
                onDelete={async (docId, _documentName) => {
                  await deleteDocument(docId);
                  window.location.reload();
                }}
              />
            </div>
          </section>

          <section className="panel-elevated flex min-h-0 flex-col">
            <div className="flex flex-col gap-3 border-b border-slate-100 px-5 py-4 sm:flex-row sm:items-center sm:justify-between lg:px-6">
              <div className="flex flex-col gap-1">
                <span className="pill w-fit">Query</span>
                <h2 className="font-display text-lg font-semibold text-slate-900">Evidence-aware chat</h2>
              </div>
              <div className="rounded-lg border border-slate-200 bg-slate-50/80 px-3 py-2 text-sm font-medium text-slate-600">
                {status
                  ? `Refinery: ${status.stage} · ${status.progress_percent}%${status.budget_status && status.budget_status !== "under_cap" ? ` · ${status.budget_status}` : ""}`
                  : "Select a document to see status."}
              </div>
            </div>

            {selectedDocId && status && (
              <div className="mx-5 mb-4 rounded-lg border border-slate-200 bg-slate-50/80 p-4 lg:mx-6">
                <h3 className="text-sm font-semibold text-slate-800">Refinery progress</h3>
                <div className="mt-3 space-y-3 text-sm">
                  <div className="flex flex-wrap items-center gap-3">
                    {(status.status === "running" || status.status === "completed" || status.status === "failed") && (
                      <>
                        <span className="rounded bg-white px-2 py-1 text-xs font-medium text-slate-500">Elapsed</span>
                        <span className="font-mono tabular-nums text-slate-800">{formatElapsed(elapsedSeconds)}</span>
                      </>
                    )}
                    <span className="rounded bg-white px-2 py-1 text-xs font-medium text-slate-500">Stage</span>
                    <span className="text-slate-700">{status.stage}</span>
                    <span className="rounded bg-white px-2 py-1 text-xs font-medium text-slate-500">Status</span>
                    <span className="text-slate-700">{status.status}</span>
                  </div>
                  {(status.status === "approval_required" || status.approval_required) && (
                    <div className="rounded border border-amber-200 bg-amber-50 p-3">
                      <p className="text-sm font-medium text-amber-900">Budget approval required</p>
                      <p className="mt-1 text-xs text-amber-800">
                        Estimated cost ${status.estimated_cost_usd?.toFixed(2) ?? "?"} exceeds budget ${status.budget_cap_usd?.toFixed(2) ?? "?"}
                        {status.page_count != null ? ` for ${status.page_count} pages.` : "."}
                      </p>
                      <button
                        type="button"
                        className="mt-2 rounded-lg bg-amber-600 px-3 py-2 text-xs font-semibold text-white hover:bg-amber-700"
                        onClick={async () => {
                          try {
                            await processDocument(selectedDocId, { approve: true, ...(languageHint ? { language_hint: languageHint } : {}) });
                          } catch {
                            // already started or error
                          }
                          const r = await listDocuments();
                          setDocuments(r.documents);
                        }}
                      >
                        Approve and continue
                      </button>
                    </div>
                  )}
                  {status.can_resume && status.status !== "running" && (
                    <div className="rounded border border-slate-200 bg-slate-50 p-3">
                      <p className="text-sm font-medium text-slate-800">Extraction paused (budget cap reached)</p>
                      <p className="mt-1 text-xs text-slate-600">Resume from last checkpoint to continue with remaining pages.</p>
                      <button
                        type="button"
                        className="mt-2 rounded-lg bg-slate-700 px-3 py-2 text-xs font-semibold text-white hover:bg-slate-800"
                        onClick={async () => {
                          try {
                            await processDocument(selectedDocId, { resume: true, ...(languageHint ? { language_hint: languageHint } : {}) });
                          } catch {
                            // already started or error
                          }
                          const r = await listDocuments();
                          setDocuments(r.documents);
                        }}
                      >
                        Resume extraction
                      </button>
                    </div>
                  )}
                  {status.status === "queued" && !status.approval_required && !status.can_resume && (
                    <button
                      type="button"
                      className="rounded-lg bg-slate-900 px-3 py-2 text-xs font-semibold text-white hover:bg-slate-800"
                      onClick={async () => {
                        try {
                          await processDocument(selectedDocId, languageHint ? { language_hint: languageHint } : undefined);
                        } catch {
                          // already started or error
                        }
                        const r = await listDocuments();
                        setDocuments(r.documents);
                      }}
                    >
                      Start processing
                    </button>
                  )}
                  <div className="flex items-center gap-2">
                    <div className="h-2 flex-1 overflow-hidden rounded-full bg-slate-200">
                      <div
                        className="h-full rounded-full bg-slate-700 transition-all duration-300"
                        style={{ width: `${status.progress_percent}%` }}
                      />
                    </div>
                    <span className="w-10 text-right text-sm font-medium tabular-nums text-slate-600">{status.progress_percent}%</span>
                  </div>
                  {status.error && (
                    <p className="rounded border border-red-200 bg-red-50 px-2 py-1.5 text-xs text-red-800">
                      {status.error}
                    </p>
                  )}
                  {status.status === "completed" && (
                    <div className="space-y-2 border-t border-slate-100 pt-3 mt-3 text-xs text-slate-600">
                      <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
                        {status.cost_estimate_usd != null && (
                          <span>
                            Cost: <span className="font-medium tabular-nums text-slate-800">${status.cost_estimate_usd.toFixed(4)}</span> USD
                            {status.cost_estimate_usd === 0 && <span className="ml-1 text-slate-500"> (local OCR, no API)</span>}
                          </span>
                        )}
                        {status.processing_time_ms != null && (
                          <span>
                            Time: <span className="font-medium tabular-nums text-slate-800">{(status.processing_time_ms / 1000).toFixed(1)}s</span>
                          </span>
                        )}
                      </div>
                      <div>
                        <span className="font-medium text-slate-700">Vision tokens:</span>{" "}
                        {(status.prompt_tokens != null || status.completion_tokens != null) ? (
                          <>
                            <span className="font-medium tabular-nums text-slate-800">{(status.prompt_tokens ?? 0).toLocaleString()} in</span>
                            {status.completion_tokens != null && (
                              <> · <span className="font-medium tabular-nums text-slate-800">{(status.completion_tokens).toLocaleString()} out</span></>
                            )}
                          </>
                        ) : (
                          <span className="text-slate-500">— (local OCR, no API call)</span>
                        )}
                      </div>
                      {(status.vision_input_per_1m_usd != null || status.vision_output_per_1m_usd != null) && (
                        <div>
                          <span className="font-medium text-slate-700">Pricing</span>
                          {status.vision_model_name && <span className="ml-1 text-slate-500">({status.vision_model_name})</span>}
                          :{" "}
                          <span className="tabular-nums text-slate-800">
                            ${status.vision_input_per_1m_usd?.toFixed(2) ?? "—"}/1M in
                            {" · "}
                            ${status.vision_output_per_1m_usd?.toFixed(2) ?? "—"}/1M out
                          </span>
                        </div>
                      )}
                      {status.cost_estimate_usd != null && status.cost_estimate_usd > 0 && status.prompt_tokens != null && status.completion_tokens != null && (
                        (() => {
                          const total = (status.prompt_tokens ?? 0) + (status.completion_tokens ?? 0);
                          if (total === 0) return null;
                          const perM = (status.cost_estimate_usd! / total) * 1e6;
                          return (
                            <div>
                              <span className="font-medium text-slate-700">Effective rate:</span>{" "}
                              <span className="font-medium tabular-nums text-slate-800">${perM.toFixed(2)}</span> / 1M tokens
                            </div>
                          );
                        })()
                      )}
                    </div>
                  )}
                </div>
              </div>
            )}

            <div className="flex min-h-0 flex-1 flex-col px-5 pb-5 lg:px-6 lg:pb-6">
              <ChatPanel
                onAsk={(query) =>
                  runQuery({
                    doc_ids: selectedDocIds,
                    query,
                    model_override: modelConfig.auto_select
                      ? undefined
                      : { provider: modelConfig.query_provider, model_name: modelConfig.query_model_name },
                  })
                }
              />
            </div>
          </section>
        </div>
      </div>
    </main>
  );
}
