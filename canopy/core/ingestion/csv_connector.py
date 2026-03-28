from __future__ import annotations

import csv
from pathlib import Path
from typing import IO, Iterator

from canopy.core.ingestion.base import BaseConnector
from canopy.models.config import SourceConfig


class CsvConnector(BaseConnector):
    """Source connector for CSV files."""

    def __init__(self, config: SourceConfig) -> None:
        self.path = Path(config.path)
        self.delimiter = config.delimiter
        self.encoding = config.encoding
        self._columns: list[str] | None = None

    def _open(self) -> IO[str]:
        if not self.path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.path}")
        return open(self.path, newline="", encoding=self.encoding)

    def get_raw_columns(self) -> list[str]:
        if self._columns is not None:
            return self._columns
        with self._open() as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            self._columns = next(reader)
        return self._columns

    def read_sample(self, n: int = 50) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        with self._open() as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            for i, row in enumerate(reader):
                if i >= n:
                    break
                rows.append(dict(row))
        return rows

    def read_all(self, chunk_size: int = 1000) -> Iterator[list[dict[str, str]]]:
        with self._open() as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            chunk: list[dict[str, str]] = []
            for row in reader:
                chunk.append(dict(row))
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk

    def get_row_count(self) -> int | None:
        count = 0
        with self._open() as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            next(reader, None)  # skip header
            for _ in reader:
                count += 1
        return count
