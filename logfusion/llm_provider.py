from __future__ import annotations

import json
import os
from typing import Any, Callable, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class LLMProviderError(RuntimeError):
    pass


class LLMProvider(Protocol):
    def generate_parser(self, request: dict[str, Any]) -> dict[str, Any]: ...


class OpenAICompatibleProvider:
    """Small dependency-free adapter for an OpenAI-compatible chat endpoint."""

    def __init__(
        self,
        base_url: str,
        model: str,
        api_key_env: str = "LOGFUSION_LLM_API_KEY",
        timeout_seconds: float = 30.0,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key_env = api_key_env
        self.timeout_seconds = timeout_seconds
        self.opener = opener

    def generate_parser(self, request: dict[str, Any]) -> dict[str, Any]:
        api_key = os.environ.get(self.api_key_env)
        if not api_key:
            raise LLMProviderError(f"Missing API key environment variable: {self.api_key_env}")
        payload = {
            "model": self.model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [{
                "role": "user",
                "content": json.dumps(request, ensure_ascii=False),
            }],
        }
        http_request = Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self.opener(http_request, timeout=self.timeout_seconds) as response:
                document = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise LLMProviderError(str(exc)) from exc
        try:
            content = document["choices"][0]["message"]["content"]
            parsed = json.loads(content) if isinstance(content, str) else content
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise LLMProviderError("Provider response does not contain a JSON parser proposal") from exc
        if not isinstance(parsed, dict):
            raise LLMProviderError("Provider proposal must be a JSON object")
        return parsed
