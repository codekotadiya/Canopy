from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from canopy.models.execution import LoadSummary
from canopy.models.schema import TargetSchema


class BaseLoader(ABC):
    """Abstract base class for all target database loaders."""

    @abstractmethod
    def get_target_schema(self, table_name: str) -> TargetSchema | None:
        """Introspect an existing table. Returns None if the table doesn't exist."""
        ...

    @abstractmethod
    def ensure_table(self, schema: TargetSchema) -> None:
        """Create the target table if it does not exist. Idempotent."""
        ...

    @abstractmethod
    def load_batch(self, table_name: str, rows: list[dict[str, Any]]) -> int:
        """Insert a batch of rows. Returns number of rows successfully inserted."""
        ...

    @abstractmethod
    def finalize(self) -> LoadSummary:
        """Commit pending work, close connections, and return a summary."""
        ...
