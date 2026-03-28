from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator


class BaseConnector(ABC):
    """Abstract base class for all source data connectors."""

    @abstractmethod
    def read_sample(self, n: int = 50) -> list[dict[str, str]]:
        """Read up to n rows for LLM analysis.

        Returns a list of dicts where keys are column names and values are raw strings.
        """
        ...

    @abstractmethod
    def read_all(self, chunk_size: int = 1000) -> Iterator[list[dict[str, str]]]:
        """Yield chunks of rows for full execution. Streaming — never loads full dataset."""
        ...

    @abstractmethod
    def get_raw_columns(self) -> list[str]:
        """Return the raw column names from the source."""
        ...

    def get_row_count(self) -> int | None:
        """Return total row count if cheaply available. None if unknown."""
        return None
