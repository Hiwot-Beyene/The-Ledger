from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass
from urllib import error, request

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

from src.models import (
    DocumentProfile,
    ModelProvider,
    ModelSelectionDecision,
    ModelSelectionMode,
)
from src.services.pricing import cost_from_usage


@dataclass
class ProviderResult:
    text: str
    estimated_cost_usd: float
    estimated_latency_ms: int
    prompt_tokens: int = 0
    completion_tokens: int = 0


class BaseProviderAdapter:
    provider: ModelProvider

    def generate(self, model_name: str, prompt: str) -> ProviderResult:
        raise NotImplementedError

    def generate_vision(self, model_name: str, prompt: str, image_b64: str) -> ProviderResult:
        raise NotImplementedError


def _http_json(url: str, method: str = "GET", headers: dict[str, str] | None = None, body: dict | None = None, timeout: int = 20) -> dict:
    req_headers = headers or {}
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"

    req = request.Request(url=url, method=method, headers=req_headers, data=payload)
    try:
        with request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code} calling {url}: {detail}") from exc
    except error.URLError as exc:
        reason = getattr(exc, "reason", None)
        if reason is not None and getattr(reason, "args", None):
            if "timed out" in str(reason).lower():
                raise RuntimeError(f"Request to {url} timed out after {timeout}s.") from exc
        raise RuntimeError(f"Unable to reach {url}: {exc.reason}") from exc


def _prioritize_models(models: list[str], preferred_prefixes: list[str]) -> list[str]:
    def score(name: str) -> tuple[int, str]:
        lower = name.lower()
        for idx, prefix in enumerate(preferred_prefixes):
            if prefix in lower:
                return (idx, lower)
        return (len(preferred_prefixes) + 1, lower)

    unique = sorted(set(models), key=score)
    return unique


def discover_ollama_models(base_url: str, api_key: str | None = None) -> list[str]:
    headers = {"Authorization": f"Bearer {api_key.strip()}"} if api_key and api_key.strip() else None
    payload = _http_json(
        f"{base_url.rstrip('/')}/api/tags",
        headers=headers,
    )
    models = [item.get("name", "") for item in payload.get("models", []) if item.get("name")]
    return _prioritize_models(models, ["llava", "qwen", "llama", "mistral", "phi", "gemma"])


def discover_openrouter_models(api_key: str, base_url: str) -> list[str]:
    if not api_key or not api_key.strip():
        return []
    url = f"{base_url.rstrip('/')}/models"
    payload = _http_json(
        url,
        method="GET",
        headers={
            "Authorization": f"Bearer {api_key.strip()}",
            "HTTP-Referer": os.getenv("OPENROUTER_REFERER", "http://localhost:3000"),
            "X-Title": os.getenv("OPENROUTER_APP_TITLE", "Document Refinery"),
        },
        timeout=25,
    )
    raw = payload.get("data")
    if raw is None and isinstance(payload, list):
        raw = payload
    if not isinstance(raw, list):
        return []
    models = []
    for item in raw:
        if isinstance(item, dict):
            mid = item.get("id") or item.get("name") or ""
        else:
            mid = str(item) if item else ""
        if mid:
            models.append(mid)
    return _prioritize_models(models, ["gpt-4o", "claude", "gemini", "qwen", "llama"])


def discover_openai_models(api_key: str, base_url: str) -> list[str]:
    if not api_key:
        return []
    payload = _http_json(
        f"{base_url.rstrip('/')}/models",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    models = [item.get("id", "") for item in payload.get("data", []) if item.get("id")]
    return _prioritize_models(models, ["gpt-4.1", "gpt-4o", "gpt-4", "gpt-3.5"])


def discover_gemini_models(api_key: str, base_url: str) -> list[str]:
    if not api_key:
        return []
    payload = _http_json(
        f"{base_url.rstrip('/')}/models?key={api_key}",
        headers={},
        timeout=25,
    )
    raw = payload.get("models", [])
    models: list[str] = []
    for item in raw:
        name = str(item.get("name", ""))
        if not name:
            continue
        models.append(name.split("/")[-1])
    if not models:
        # Keep sane defaults even when listing is restricted.
        models = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-pro"]
    return _prioritize_models(models, ["gemini-2.5", "gemini-2.0", "gemini-1.5"])


OLLAMA_GENERATE_TIMEOUT = 3600
OLLAMA_VISION_TIMEOUT = 3600

class OllamaAdapter(BaseProviderAdapter):
    provider = ModelProvider.OLLAMA

    def __init__(self, base_url: str, api_key: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = (api_key or "").strip() or None

    def _headers(self) -> dict[str, str] | None:
        if self.api_key:
            return {"Authorization": f"Bearer {self.api_key}"}
        return None

    def generate(self, model_name: str, prompt: str) -> ProviderResult:
        started = time.perf_counter()
        payload = _http_json(
            f"{self.base_url}/api/generate",
            method="POST",
            headers=self._headers(),
            body={"model": model_name, "prompt": prompt, "stream": False},
            timeout=OLLAMA_GENERATE_TIMEOUT,
        )
        prompt_tokens = int(payload.get("prompt_eval_count") or 0)
        completion_tokens = int(payload.get("eval_count") or 0)
        return ProviderResult(
            text=str(payload.get("response", "")),
            estimated_cost_usd=0.0,
            estimated_latency_ms=int((time.perf_counter() - started) * 1000),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

    def generate_vision(self, model_name: str, prompt: str, image_b64: str) -> ProviderResult:
        started = time.perf_counter()
        payload = _http_json(
            f"{self.base_url}/api/generate",
            method="POST",
            headers=self._headers(),
            body={
                "model": model_name,
                "prompt": prompt,
                "images": [image_b64],
                "stream": False,
            },
            timeout=OLLAMA_VISION_TIMEOUT,
        )
        prompt_tokens = int(payload.get("prompt_eval_count") or 0)
        completion_tokens = int(payload.get("eval_count") or 0)
        return ProviderResult(
            text=str(payload.get("response", "")),
            estimated_cost_usd=0.0,
            estimated_latency_ms=int((time.perf_counter() - started) * 1000),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )


class OpenRouterAdapter(BaseProviderAdapter):
    provider = ModelProvider.OPENROUTER

    def __init__(self, api_key: str, base_url: str):
        if OpenAI is None:
            raise ImportError("openai package is required for OpenRouter adapter")
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")
        default_headers = {
            "HTTP-Referer": os.getenv("OPENROUTER_REFERER", "http://localhost:3000"),
            "X-Title": os.getenv("OPENROUTER_APP_TITLE", "Document Refinery"),
        }
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            default_headers=default_headers,
        )

    def generate(self, model_name: str, prompt: str) -> ProviderResult:
        started = time.perf_counter()
        response = self.client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        content = response.choices[0].message.content or ""
        usage = getattr(response, "usage", None)
        prompt_tokens = int(usage.prompt_tokens) if usage and getattr(usage, "prompt_tokens", None) is not None else 0
        completion_tokens = int(usage.completion_tokens) if usage and getattr(usage, "completion_tokens", None) is not None else 0
        return ProviderResult(
            text=content,
            estimated_cost_usd=0.0,
            estimated_latency_ms=int((time.perf_counter() - started) * 1000),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

    def generate_vision(self, model_name: str, prompt: str, image_b64: str) -> ProviderResult:
        started = time.perf_counter()
        response = self.client.chat.completions.create(
            model=model_name,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
                    ],
                }
            ],
            temperature=0.0,
        )
        content = response.choices[0].message.content or ""
        usage = getattr(response, "usage", None)
        prompt_tokens = int(usage.prompt_tokens) if usage and getattr(usage, "prompt_tokens", None) is not None else 0
        completion_tokens = int(usage.completion_tokens) if usage and getattr(usage, "completion_tokens", None) is not None else 0
        return ProviderResult(
            text=content,
            estimated_cost_usd=0.0,
            estimated_latency_ms=int((time.perf_counter() - started) * 1000),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )


class OpenAIAdapter(BaseProviderAdapter):
    provider = ModelProvider.OPENAI

    def __init__(self, api_key: str, base_url: str):
        if OpenAI is None:
            raise ImportError("openai package is required for OpenAI adapter")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.client = OpenAI(api_key=api_key, base_url=self.base_url)

    def generate(self, model_name: str, prompt: str) -> ProviderResult:
        started = time.perf_counter()
        response = self.client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        content = response.choices[0].message.content or ""
        usage = getattr(response, "usage", None)
        prompt_tokens = int(usage.prompt_tokens) if usage and getattr(usage, "prompt_tokens", None) is not None else 0
        completion_tokens = int(usage.completion_tokens) if usage and getattr(usage, "completion_tokens", None) is not None else 0
        return ProviderResult(
            text=content,
            estimated_cost_usd=0.0,
            estimated_latency_ms=int((time.perf_counter() - started) * 1000),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

    def generate_vision(self, model_name: str, prompt: str, image_b64: str) -> ProviderResult:
        started = time.perf_counter()
        response = self.client.chat.completions.create(
            model=model_name,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
                    ],
                }
            ],
            temperature=0.0,
        )
        content = response.choices[0].message.content or ""
        usage = getattr(response, "usage", None)
        prompt_tokens = int(usage.prompt_tokens) if usage and getattr(usage, "prompt_tokens", None) is not None else 0
        completion_tokens = int(usage.completion_tokens) if usage and getattr(usage, "completion_tokens", None) is not None else 0
        return ProviderResult(
            text=content,
            estimated_cost_usd=0.0,
            estimated_latency_ms=int((time.perf_counter() - started) * 1000),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )


class GeminiAdapter(BaseProviderAdapter):
    provider = ModelProvider.GEMINI

    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")

    def _url(self, model_name: str) -> str:
        return f"{self.base_url}/models/{model_name}:generateContent?key={self.api_key}"

    def generate(self, model_name: str, prompt: str) -> ProviderResult:
        started = time.perf_counter()
        payload = _http_json(
            self._url(model_name),
            method="POST",
            body={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=120,
        )
        text = ""
        prompt_tokens = int(payload.get("usageMetadata", {}).get("promptTokenCount", 0) or 0)
        completion_tokens = int(payload.get("usageMetadata", {}).get("candidatesTokenCount", 0) or 0)
        candidates = payload.get("candidates", []) or []
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            text = "".join(str(p.get("text", "")) for p in parts if isinstance(p, dict))
        return ProviderResult(
            text=text,
            estimated_cost_usd=0.0,
            estimated_latency_ms=int((time.perf_counter() - started) * 1000),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

    def generate_vision(self, model_name: str, prompt: str, image_b64: str) -> ProviderResult:
        started = time.perf_counter()
        payload = _http_json(
            self._url(model_name),
            method="POST",
            body={
                "contents": [
                    {
                        "parts": [
                            {"text": prompt},
                            {
                                "inline_data": {
                                    "mime_type": "image/png",
                                    "data": image_b64,
                                }
                            },
                        ]
                    }
                ]
            },
            timeout=180,
        )
        text = ""
        prompt_tokens = int(payload.get("usageMetadata", {}).get("promptTokenCount", 0) or 0)
        completion_tokens = int(payload.get("usageMetadata", {}).get("candidatesTokenCount", 0) or 0)
        candidates = payload.get("candidates", []) or []
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            text = "".join(str(p.get("text", "")) for p in parts if isinstance(p, dict))
        return ProviderResult(
            text=text,
            estimated_cost_usd=0.0,
            estimated_latency_ms=int((time.perf_counter() - started) * 1000),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )


class ModelGateway:
    def __init__(self, rules: dict, runtime_config: dict | None = None):
        self.rules = rules
        self.runtime_config = runtime_config or {}
        self.live_model_calls = bool(self.runtime_config.get("live_model_calls", False))
        base = self.runtime_config.get("ollama_base_url") or os.getenv("OLLAMA_BASE_URL", "")
        self.ollama_base_url = str(base).strip() if str(base).strip() else "http://localhost:11434"
        if "api.ollama.com" in self.ollama_base_url:
            self.ollama_base_url = self.ollama_base_url.replace("api.ollama.com", "ollama.com", 1)
        self.ollama_api_key = str(self.runtime_config.get("ollama_api_key") or os.getenv("OLLAMA_API_KEY", "")).strip() or ""
        self.openrouter_base_url = str(
            self.runtime_config.get("openrouter_base_url") or os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
        ).strip() or "https://openrouter.ai/api/v1"
        self.openai_base_url = str(os.getenv("OPENAI_BASE_URL", self.runtime_config.get("openai_base_url", "https://api.openai.com/v1")))
        self.gemini_base_url = str(
            os.getenv("GEMINI_BASE_URL", self.runtime_config.get("gemini_base_url", "https://generativelanguage.googleapis.com/v1beta"))
        ).strip() or "https://generativelanguage.googleapis.com/v1beta"
        self.openrouter_api_key = str(self.runtime_config.get("openrouter_api_key") or os.getenv("OPENROUTER_API_KEY", ""))
        self.openai_api_key = str(self.runtime_config.get("openai_api_key") or os.getenv("OPENAI_API_KEY", ""))
        self.gemini_api_key = str(self.runtime_config.get("gemini_api_key") or os.getenv("GEMINI_API_KEY", ""))

        self.providers: dict[ModelProvider, BaseProviderAdapter] = {}
        if self.gemini_api_key:
            self.providers[ModelProvider.GEMINI] = GeminiAdapter(
                api_key=self.gemini_api_key,
                base_url=self.gemini_base_url,
            )
        self.providers[ModelProvider.OLLAMA] = OllamaAdapter(base_url=self.ollama_base_url, api_key=self.ollama_api_key or None)
        if self.openrouter_api_key:
            self.providers[ModelProvider.OPENROUTER] = OpenRouterAdapter(
                api_key=self.openrouter_api_key,
                base_url=self.openrouter_base_url,
            )
        if self.openai_api_key:
            self.providers[ModelProvider.OPENAI] = OpenAIAdapter(
                api_key=self.openai_api_key,
                base_url=self.openai_base_url,
            )

    @staticmethod
    def is_paid_provider(provider: ModelProvider) -> bool:
        return provider in {ModelProvider.OPENROUTER, ModelProvider.OPENAI, ModelProvider.GEMINI}

    def discover_catalog(self) -> tuple[list[dict], dict[str, str]]:
        errors: dict[str, str] = {}
        try:
            ollama_models = discover_ollama_models(self.ollama_base_url, api_key=self.ollama_api_key or None)
        except Exception as e:
            ollama_models = []
            errors["ollama"] = str(e)

        try:
            openrouter_models = discover_openrouter_models(self.openrouter_api_key, self.openrouter_base_url)
        except Exception as e:
            openrouter_models = []
            errors["openrouter"] = str(e)

        try:
            openai_models = discover_openai_models(self.openai_api_key, self.openai_base_url)
        except Exception as e:
            openai_models = []
            errors["openai"] = str(e)
        try:
            gemini_models = discover_gemini_models(self.gemini_api_key, self.gemini_base_url)
        except Exception as e:
            gemini_models = []
            errors["gemini"] = str(e)

        providers = [
            {
                "provider": "ollama",
                "paid": False,
                "requires_api_key": False,
                "key_configured": True,
                "models": ollama_models,
                "has_ollama_api_key": bool(self.ollama_api_key),
            },
            {
                "provider": "openrouter",
                "paid": True,
                "requires_api_key": True,
                "key_configured": bool(self.openrouter_api_key),
                "models": openrouter_models,
            },
            {
                "provider": "openai",
                "paid": True,
                "requires_api_key": True,
                "key_configured": bool(self.openai_api_key),
                "models": openai_models,
            },
            {
                "provider": "gemini",
                "paid": True,
                "requires_api_key": True,
                "key_configured": bool(self.gemini_api_key),
                "models": gemini_models,
            },
        ]
        return providers, errors

    def recommend(
        self,
        profile: DocumentProfile | None,
        query: str,
    ) -> tuple[ModelProvider, str, str]:
        cfg = self.rules.get("model_selection", {})
        default_provider = ModelProvider(str(cfg.get("default_provider", "gemini")))
        default_model = str(cfg.get("default_model", "gemini-2.5-flash"))
        vision_provider = ModelProvider(str(cfg.get("vision_provider", "gemini")))
        vision_model = str(cfg.get("vision_model", "gemini-2.5-flash"))

        reason = "default policy"
        if profile and str(profile.origin_type.value) in {"scanned_image", "form_fillable"}:
            return vision_provider, vision_model, "vision for scanned/form profile"

        lower_query = (query or "").lower()
        if "table" in lower_query or "figure" in lower_query:
            return vision_provider, vision_model, "vision for structure-heavy question"

        return default_provider, default_model, reason

    def select_model(
        self,
        query: str,
        profile: DocumentProfile | None = None,
        override: dict | None = None,
        doc_id: str | None = None,
        query_id: str | None = None,
    ) -> ModelSelectionDecision:
        if override:
            provider = ModelProvider(str(override["provider"]))
            model_name = str(override["model_name"])
            mode = ModelSelectionMode.USER_OVERRIDE
            reason = "user override"
        else:
            provider, model_name, reason = self.recommend(profile=profile, query=query)
            mode = ModelSelectionMode.AUTO

        if self.live_model_calls:
            adapter = self.providers.get(provider)
            if adapter is None:
                raise ValueError(
                    f"Provider '{provider.value}' is not configured. Configure required API key in model settings."
                )
            result = adapter.generate(model_name=model_name, prompt=query)
        else:
            estimated_cost = 0.0 if not self.is_paid_provider(provider) else 0.02
            estimated_latency = 220 if provider == ModelProvider.OLLAMA else 480
            result = ProviderResult(
                text="",
                estimated_cost_usd=estimated_cost,
                estimated_latency_ms=estimated_latency,
            )

        return ModelSelectionDecision(
            decision_id=f"dec-{(query_id or doc_id or 'global')[:12]}",
            provider=provider,
            model_name=model_name,
            mode=mode,
            reasoning=reason,
            estimated_cost_usd=result.estimated_cost_usd,
            estimated_latency_ms=result.estimated_latency_ms,
            doc_id=doc_id,
            query_id=query_id,
        )

    def select_vision_model(self, override: dict | None = None) -> tuple[ModelProvider, str]:
        cfg = self.rules.get("model_selection", {})
        provider = ModelProvider(str(cfg.get("vision_provider", "gemini")))
        model_name = str(cfg.get("vision_model", "gemini-2.5-flash"))
        runtime_vision = self.runtime_config.get("vision_override")
        if runtime_vision:
            override = runtime_vision
        if override:
            provider = ModelProvider(str(override.get("provider", provider.value)))
            model_name = str(override.get("model_name", model_name))
        return provider, model_name

    def generate_vision(self, provider: ModelProvider, model_name: str, prompt: str, image_bytes: bytes) -> ProviderResult:
        adapter = self.providers.get(provider)
        if adapter is None:
            raise ValueError(
                f"Provider '{provider.value}' is not configured. If paid provider is selected, provide API key in model config."
            )

        image_b64 = base64.b64encode(image_bytes).decode("utf-8")
        result = adapter.generate_vision(model_name=model_name, prompt=prompt, image_b64=image_b64)
        cost = cost_from_usage(
            result.prompt_tokens,
            result.completion_tokens,
            provider,
            model_name,
            self.rules,
            self.runtime_config,
        )
        return ProviderResult(
            text=result.text,
            estimated_cost_usd=cost,
            estimated_latency_ms=result.estimated_latency_ms,
            prompt_tokens=result.prompt_tokens,
            completion_tokens=result.completion_tokens,
        )
