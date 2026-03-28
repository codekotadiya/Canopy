"""JSON source connector for Canopy pipelines."""

from __future__ import annotations

import json
from pathlib import Path
from typing import IO, Any, Iterator

from canopy.core.ingestion.base import BaseConnector
from canopy.models.config import SourceConfig


class JsonConnector(BaseConnector):
    """Source connector for JSON files.

    Supports:
    - JSON array of objects: ``[{"col": "val"}, ...]``
    - Newline-delimited JSON (NDJSON): one object per line
    """

    def __init__(self, config: SourceConfig) -> None:
        self.path = Path(config.path)
        self.encoding = config.encoding
        self._records: list[dict[str, str]] | None = None
        self._columns: list[str] | None = None

    def _open(self) -> IO[str]:
        if not self.path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.path}")
        return open(self.path, encoding=self.encoding)

    def _load_records(self) -> list[dict[str, str]]:
        """Load all records, converting values to strings (matching CSV behavior)."""
        if self._records is not None:
            return self._records

        with self._open() as f:
            text = f.read().strip()

        if not text:
            raise ValueError(f"JSON file is empty: {self.path}")

        # Try JSON array first, then NDJSON
        if text.startswith("["):
            raw: list[dict[str, Any]] = json.loads(text)
        else:
            raw = []
            for i, line in enumerate(text.splitlines(), 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    raw.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSON on line {i} of {self.path}: {exc}"
                    ) from exc

        if not raw:
            raise ValueError(f"JSON file contains no records: {self.path}")

        if not isinstance(raw[0], dict):
            raise ValueError(
                f"Expected array of objects, got array of {type(raw[0]).__name__}"
            )

        # Stringify all values to match CSV connector output format
        self._records = [
            {k: str(v) if v is not None else "" for k, v in record.items()}
            for record in raw
        ]

        return self._records

    def get_raw_columns(self) -> list[str]:
        if self._columns is not None:
            return self._columns
        records = self._load_records()
        # Collect all keys across records (preserving order from first record)
        seen: dict[str, None] = {}
        for record in records:
            for key in record:
                if key not in seen:
                    seen[key] = None
        self._columns = list(seen)
        return self._columns

    def read_sample(self, n: int = 50) -> list[dict[str, str]]:
        return self._load_records()[:n]

    def read_all(self, chunk_size: int = 1000) -> Iterator[list[dict[str, str]]]:
        records = self._load_records()
        for i in range(0, len(records), chunk_size):
            yield records[i : i + chunk_size]

    def get_row_count(self) -> int | None:
        return len(self._load_records())
