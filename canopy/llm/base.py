from __future__ import annotations

from abc import ABC, abstractmethod


class BaseLLMProvider(ABC):
    """Abstract base class for all LLM providers."""

    @abstractmethod
    def complete(self, prompt: str, system: str | None = None) -> str:
        """Send a prompt to the LLM and return the text response."""
        ...

    @abstractmethod
    def is_cloud(self) -> bool:
        """Return True if data leaves the local machine (triggers privacy warning)."""
        ...
