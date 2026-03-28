from __future__ import annotations

import csv
from pathlib import Path

import pytest

from canopy.core.ingestion.csv_connector import CsvConnector
from canopy.models.config import SourceConfig


def _make_config(path: Path, **kwargs) -> SourceConfig:
    return SourceConfig(path=path, **kwargs)


class TestCsvConnector:
    def test_get_raw_columns(self, sample_csv_path: Path):
        conn = CsvConnector(_make_config(sample_csv_path))
        cols = conn.get_raw_columns()
        assert cols == ["Full Name", "Email", "Phone", "Hire Date", "Salary", "Active"]

    def test_read_sample_returns_correct_count(self, sample_csv_path: Path):
        conn = CsvConnector(_make_config(sample_csv_path))
        sample = conn.read_sample(3)
        assert len(sample) == 3
        assert sample[0]["Full Name"] == "John Smith"

    def test_read_sample_returns_all_if_n_exceeds_rows(self, sample_csv_path: Path):
        conn = CsvConnector(_make_config(sample_csv_path))
        sample = conn.read_sample(100)
        assert len(sample) == 5  # only 5 rows in fixture

    def test_read_all_single_chunk(self, sample_csv_path: Path):
        conn = CsvConnector(_make_config(sample_csv_path))
        chunks = list(conn.read_all(chunk_size=100))
        assert len(chunks) == 1
        assert len(chunks[0]) == 5

    def test_read_all_multiple_chunks(self, sample_csv_path: Path):
        conn = CsvConnector(_make_config(sample_csv_path))
        chunks = list(conn.read_all(chunk_size=2))
        assert len(chunks) == 3  # 2 + 2 + 1
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 2
        assert len(chunks[2]) == 1

    def test_get_row_count(self, sample_csv_path: Path):
        conn = CsvConnector(_make_config(sample_csv_path))
        assert conn.get_row_count() == 5

    def test_missing_file_raises(self, tmp_path: Path):
        conn = CsvConnector(_make_config(tmp_path / "nonexistent.csv"))
        with pytest.raises(FileNotFoundError):
            conn.read_sample()

    def test_custom_delimiter(self, tmp_path: Path):
        tsv_path = tmp_path / "data.tsv"
        with open(tsv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["name", "age"])
            writer.writerow(["Alice", "30"])
            writer.writerow(["Bob", "25"])

        conn = CsvConnector(_make_config(tsv_path, delimiter="\t"))
        sample = conn.read_sample()
        assert len(sample) == 2
        assert sample[0]["name"] == "Alice"
        assert sample[0]["age"] == "30"

    def test_empty_csv_returns_empty(self, tmp_path: Path):
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("name,age\n", encoding="utf-8")
        conn = CsvConnector(_make_config(csv_path))
        assert conn.read_sample() == []
        assert conn.get_row_count() == 0
        assert list(conn.read_all()) == []
