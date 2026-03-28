from __future__ import annotations

import httpx

from canopy.llm.base import BaseLLMProvider
from canopy.models.config import LLMConfig


class OllamaProvider(BaseLLMProvider):
    """LLM provider for locally-hosted Ollama models."""

    def __init__(self, config: LLMConfig) -> None:
        self.base_url = config.base_url.rstrip("/")
        self.model = config.model
        self.temperature = config.temperature
        self.timeout = config.timeout

    def complete(self, prompt: str, system: str | None = None) -> str:
        payload: dict = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": self.temperature},
        }
        if system:
            payload["system"] = system

        response = httpx.post(
            f"{self.base_url}/api/generate",
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()["response"]

    def is_cloud(self) -> bool:
        return False

    def health_check(self) -> bool:
        """Check if Ollama is reachable."""
        try:
            response = httpx.get(f"{self.base_url}/api/tags", timeout=10)
            return response.status_code == 200
        except httpx.HTTPError:
            return False
