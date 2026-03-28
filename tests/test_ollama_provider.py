from __future__ import annotations

import httpx
import pytest
import respx

from canopy.llm.ollama import OllamaProvider
from canopy.models.config import LLMConfig


@pytest.fixture
def provider() -> OllamaProvider:
    return OllamaProvider(LLMConfig(base_url="http://localhost:11434", model="llama3"))


class TestOllamaProvider:
    @respx.mock
    def test_complete_success(self, provider: OllamaProvider):
        respx.post("http://localhost:11434/api/generate").mock(
            return_value=httpx.Response(
                200, json={"response": '{"columns": []}', "done": True}
            )
        )
        result = provider.complete("test prompt")
        assert result == '{"columns": []}'

    @respx.mock
    def test_complete_with_system_prompt(self, provider: OllamaProvider):
        route = respx.post("http://localhost:11434/api/generate").mock(
            return_value=httpx.Response(200, json={"response": "ok", "done": True})
        )
        provider.complete("test", system="You are helpful")
        request = route.calls[0].request
        body = request.content.decode()
        assert '"system"' in body
        assert "You are helpful" in body

    @respx.mock
    def test_complete_http_error_raises(self, provider: OllamaProvider):
        respx.post("http://localhost:11434/api/generate").mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )
        with pytest.raises(httpx.HTTPStatusError):
            provider.complete("test")

    @respx.mock
    def test_health_check_healthy(self, provider: OllamaProvider):
        respx.get("http://localhost:11434/api/tags").mock(
            return_value=httpx.Response(200, json={"models": []})
        )
        assert provider.health_check() is True

    @respx.mock
    def test_health_check_unreachable(self, provider: OllamaProvider):
        respx.get("http://localhost:11434/api/tags").mock(
            side_effect=httpx.ConnectError("refused")
        )
        assert provider.health_check() is False

    def test_is_cloud_returns_false(self, provider: OllamaProvider):
        assert provider.is_cloud() is False
