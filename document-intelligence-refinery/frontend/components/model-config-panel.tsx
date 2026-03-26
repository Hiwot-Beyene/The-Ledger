"use client";

import React, { useEffect, useMemo, useState } from "react";

export type ModelOverride = {
  provider: "ollama" | "openrouter" | "openai";
  model_name: string;
};

export type ModelConfig = {
  auto_select: boolean;
  query_provider: "ollama" | "openrouter" | "openai";
  query_model_name: string;
  vision_provider: "ollama" | "openrouter" | "openai";
  vision_model_name: string;
  vision_source: "local" | "cloud";
  summary_provider: "ollama" | "openrouter" | "openai";
  summary_model_name: string;
  openrouter_api_key?: string;
  openrouter_base_url?: string;
  openai_api_key?: string;
  max_vision_budget_usd?: number;
  require_approval_over_budget?: boolean;
};

export type ProviderCatalog = {
  provider: "ollama" | "openrouter" | "openai";
  paid: boolean;
  requires_api_key: boolean;
  key_configured: boolean;
  models: string[];
};

type Props = {
  initial: ModelConfig;
  providers: ProviderCatalog[];
  discoveryErrors?: Record<string, string>;
  onSave: (config: ModelConfig) => Promise<void>;
  onRefresh?: () => Promise<void>;
};

export function ModelConfigPanel({ initial, providers, discoveryErrors = {}, onSave, onRefresh }: Props) {
  const [config, setConfig] = useState<ModelConfig>(initial);
  const [isEditing, setIsEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saveSuccess, setSaveSuccess] = useState(false);
  useEffect(() => {
    setConfig(initial);
  }, [initial]);

  const handleSave = async () => {
    setSaving(true);
    setSaveError(null);
    setSaveSuccess(false);
    try {
      await onSave(config);
      setSaveSuccess(true);
      setIsEditing(false);
      setTimeout(() => setSaveSuccess(false), 3000);
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : "Save failed");
    } finally {
      setSaving(false);
    }
  };

  const queryProviderEntry = useMemo(
    () => providers.find((p) => p.provider === "ollama"),
    [providers],
  );
  const visionProviderEntry = useMemo(
    () => providers.find((p) => p.provider === "ollama"),
    [providers],
  );
  const summaryProviderEntry = useMemo(
    () => providers.find((p) => p.provider === "ollama"),
    [providers],
  );
  const queryModels = queryProviderEntry?.models ?? [];
  const visionModels = visionProviderEntry?.models ?? [];
  const summaryModels = summaryProviderEntry?.models ?? [];
  const queryModelValid = config.query_model_name && queryModels.includes(config.query_model_name);
  const visionModelValid = config.vision_model_name && visionModels.includes(config.vision_model_name);
  const summaryModelValid = config.summary_model_name && summaryModels.includes(config.summary_model_name);

  const providerLabel = (p: "ollama" | "openrouter" | "openai") => {
    const entry = providers.find((x) => x.provider === p);
    const count = Array.isArray(entry?.models) ? entry.models.length : 0;
    if (p === "openrouter" || p === "openai") {
      return count > 0 ? `${p === "openrouter" ? "OpenRouter" : "OpenAI"} (${count} models)` : p === "openrouter" ? "OpenRouter (API key required)" : "OpenAI (API key required)";
    }
    return `Ollama (${count} models)`;
  };

  useEffect(() => {
    if (queryModels.length && !queryModelValid) {
      setConfig((prev) => ({ ...prev, query_model_name: queryModels[0] }));
    }
  }, [queryModels, queryModelValid]);
  useEffect(() => {
    if (config.vision_source === "cloud" && config.vision_model_name !== "qwen3-vl:235b-instruct-cloud") {
      setConfig((prev) => ({ ...prev, vision_model_name: "qwen3-vl:235b-instruct-cloud" }));
    }
  }, [config.vision_source]);
  useEffect(() => {
    if (visionModels.length && config.vision_source === "local" && !visionModels.includes(config.vision_model_name)) {
      const defaultVision = visionModels.includes("llava:7b") ? "llava:7b" : visionModels[0];
      setConfig((prev) => ({ ...prev, vision_model_name: defaultVision ?? prev.vision_model_name }));
    }
  }, [visionModels, config.vision_source, config.vision_model_name]);
  useEffect(() => {
    if (summaryModels.length && !summaryModelValid) {
      setConfig((prev) => ({ ...prev, summary_model_name: summaryModels[0] ?? prev.summary_model_name }));
    }
  }, [summaryModels, summaryModelValid]);

  return (
    <section className="border-t border-slate-200 pt-4">
      <div className="flex items-center justify-between gap-2">
        <h2 className="text-base font-semibold text-slate-900">Model</h2>
        {!isEditing ? (
          <button
            type="button"
            className="rounded-lg p-2 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
            onClick={() => setIsEditing(true)}
            title="Edit model configuration"
            aria-label="Edit model configuration"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5Z" />
              <path d="m15 5 4 4" />
            </svg>
          </button>
        ) : (
          <button
            type="button"
            className="rounded-lg px-2 py-1 text-sm text-slate-500 hover:bg-slate-100 hover:text-slate-700"
            onClick={() => { setIsEditing(false); setSaveError(null); }}
          >
            Cancel
          </button>
        )}
      </div>
          <p className="mb-2 text-xs text-slate-500">Dynamic model routing with local-first defaults.</p>
          <p className="mb-2 text-xs text-slate-500">Ollama API key (e.g. Ollama Cloud) is read from OLLAMA_API_KEY in the server .env.</p>
      {Object.keys(discoveryErrors).length > 0 && (
        <div className="mb-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          {Object.entries(discoveryErrors).map(([provider, err]) => (
            <div key={provider}>
              <strong>{provider}:</strong> {err}
            </div>
          ))}
          <p className="mt-1">Set Ollama URL below if the backend cannot reach localhost:11434 (e.g. Docker).</p>
        </div>
      )}
      <div className="mt-2">
        <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
          Ollama URL
        </label>
        <div className="mt-1 flex gap-2">
          <input
            className="flex-1 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-500"
            type="url"
            placeholder="http://localhost:11434"
            value={config.ollama_base_url ?? ""}
            onChange={(e) =>
              setConfig((prev) => ({ ...prev, ollama_base_url: e.target.value || undefined }))
            }
            disabled={!isEditing}
          />
          {onRefresh && (
            <button
              type="button"
              className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
              onClick={() => onRefresh()}
              disabled={!isEditing}
            >
              Refresh
            </button>
          )}
        </div>
      </div>
      <label className="mt-3 inline-flex items-center gap-2 text-sm font-medium">
        <input
          className="h-4 w-4 rounded border-slate-300 disabled:cursor-not-allowed"
          type="checkbox"
          checked={config.auto_select}
          onChange={(e) =>
            setConfig((prev) => ({ ...prev, auto_select: e.target.checked }))
          }
          disabled={!isEditing}
        />
        Auto select
      </label>

      <div className="mt-4 border-t border-slate-200 pt-3">
        <h3 className="text-sm font-semibold text-slate-800">Query model</h3>
        <p className="mt-0.5 text-xs text-slate-500">Always uses local Ollama.</p>
        <div className="mt-2">
          <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">Model</label>
          <div className="mt-1 flex gap-2">
            <select
              className="flex-1 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-500"
              value={config.query_model_name}
              onChange={(e) =>
                setConfig((prev) => ({ ...prev, query_model_name: e.target.value }))
              }
              disabled={!isEditing}
            >
              {queryModels.length > 0 ? (
                queryModels.map((name) => (
                  <option key={name} value={name}>{name}</option>
                ))
              ) : (
                <option value="">No models — start Ollama and refresh</option>
              )}
            </select>
            {onRefresh && (
              <button
                type="button"
                className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
                onClick={() => onRefresh()}
                disabled={!isEditing}
              >
                Refresh
              </button>
            )}
          </div>
        </div>
      </div>

      <div className="mt-4 border-t border-slate-200 pt-3">
        <h3 className="text-sm font-semibold text-slate-800">Vision model</h3>
        <p className="mt-0.5 text-xs text-slate-500">Used for scanned/image extraction. Local or Ollama Cloud.</p>
        <div className="mt-2">
          <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">Source</label>
          <select
            className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-500"
            value={config.vision_source}
            onChange={(e) => {
              const src = e.target.value as "local" | "cloud";
              setConfig((prev) => ({
                ...prev,
                vision_source: src,
                vision_model_name: src === "cloud" ? "qwen3-vl:235b-instruct-cloud" : (prev.vision_model_name || "llava:7b"),
              }));
            }}
            disabled={!isEditing}
          >
            <option value="local">Local Ollama</option>
            <option value="cloud">Ollama Cloud</option>
          </select>
        </div>
        {config.vision_source === "local" ? (
          <div className="mt-2">
            <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">Model</label>
            <select
              className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-500"
              value={config.vision_model_name}
              onChange={(e) =>
                setConfig((prev) => ({ ...prev, vision_model_name: e.target.value }))
              }
              disabled={!isEditing}
            >
              {visionModels.length > 0 ? (
                visionModels.map((name) => (
                  <option key={name} value={name}>{name}</option>
                ))
              ) : (
                <option value="llava:7b">llava:7b (refresh to load models)</option>
              )}
            </select>
          </div>
        ) : (
          <div className="mt-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            <span className="font-medium">qwen3-vl:235b-instruct-cloud</span>
            <p className="mt-1 text-xs text-slate-500">Auth via OLLAMA_API_KEY in server .env</p>
          </div>
        )}
      </div>

      <div className="mt-4 border-t border-slate-200 pt-3">
        <h3 className="text-sm font-semibold text-slate-800">Summary model</h3>
        <p className="mt-0.5 text-xs text-slate-500">PageIndex section summaries. Always uses local Ollama.</p>
        <div className="mt-2">
          <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">Model</label>
          <select
            className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-500"
            value={config.summary_model_name}
            onChange={(e) =>
              setConfig((prev) => ({ ...prev, summary_model_name: e.target.value }))
            }
            disabled={!isEditing}
          >
            {summaryModels.length > 0 ? (
              summaryModels.map((name) => (
                <option key={name} value={name}>{name}</option>
              ))
            ) : (
              <option value={config.summary_model_name || ""}>{config.summary_model_name || "Same as query"}</option>
            )}
          </select>
        </div>
      </div>

      {(config.query_provider === "openrouter" || config.vision_provider === "openrouter") && (
        <div className="mt-3 space-y-2">
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
              OpenRouter API URL
            </label>
            <input
              className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-500"
              type="url"
              placeholder="https://openrouter.ai/api/v1"
              value={config.openrouter_base_url ?? ""}
              onChange={(e) =>
                setConfig((prev) => ({ ...prev, openrouter_base_url: e.target.value || undefined }))
              }
              disabled={!isEditing}
            />
          </div>
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
              OpenRouter API Key
            </label>
            <p className="mb-1 text-xs text-slate-500">
              <a
                href="https://openrouter.ai/keys"
                target="_blank"
                rel="noopener noreferrer"
                className="underline hover:text-slate-800"
              >
                Get API key
              </a>
              {" "}· 400+ models via one API
            </p>
            <input
              className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-500"
              type="password"
              autoComplete="off"
              placeholder="sk-or-..."
              value={config.openrouter_api_key ?? ""}
              onChange={(e) =>
                setConfig((prev) => ({ ...prev, openrouter_api_key: e.target.value }))
              }
              disabled={!isEditing}
            />
          </div>
        </div>
      )}
      {(config.query_provider === "openai" || config.vision_provider === "openai") && (
        <div className="mt-3">
          <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">OpenAI Key</label>
          <input
            className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-500"
            type="password"
            value={config.openai_api_key ?? ""}
            onChange={(e) =>
              setConfig((prev) => ({ ...prev, openai_api_key: e.target.value }))
            }
            disabled={!isEditing}
          />
        </div>
      )}
      <div className="mt-3">
        <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
          Vision budget (USD per doc)
        </label>
        <input
          className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-500"
          type="number"
          min={0}
          step={0.01}
          value={config.max_vision_budget_usd ?? 2.0}
          onChange={(e) =>
            setConfig((prev) => ({
              ...prev,
              max_vision_budget_usd: Number(e.target.value),
            }))
          }
          disabled={!isEditing}
        />
      </div>
      <div className="mt-3 flex items-center gap-2">
        <input
          id="require-approval-over-budget"
          type="checkbox"
          checked={config.require_approval_over_budget ?? false}
          onChange={(e) =>
            setConfig((prev) => ({
              ...prev,
              require_approval_over_budget: e.target.checked,
            }))
          }
          disabled={!isEditing}
          className="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-500"
        />
        <label htmlFor="require-approval-over-budget" className="text-sm text-slate-700">
          Require approval when estimated cost exceeds budget
        </label>
      </div>
      {isEditing && (
        <button
          className="mt-4 w-full rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:opacity-60"
          type="button"
          disabled={saving}
          onClick={handleSave}
        >
          {saving ? "Saving…" : "Save"}
        </button>
      )}
      {saveSuccess && (
        <p className="mt-2 text-center text-sm text-green-700">Saved.</p>
      )}
      {saveError && (
        <p className="mt-2 text-center text-sm text-red-600">{saveError}</p>
      )}
    </section>
  );
}
