"""Tests for the JSON source connector."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from canopy.core.ingestion.json_connector import JsonConnector
from canopy.models.config import SourceConfig


def _write_json(tmp_path: Path, data, name: str = "data.json") -> Path:
    p = tmp_path / name
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def _write_ndjson(tmp_path: Path, records: list[dict], name: str = "data.ndjson") -> Path:
    p = tmp_path / name
    p.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    return p


class TestJsonConnector:
    def test_read_json_array(self, tmp_path: Path):
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        path = _write_json(tmp_path, data)
        conn = JsonConnector(SourceConfig(type="json", path=path))
        assert conn.get_raw_columns() == ["name", "age"]
        rows = conn.read_sample()
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == "30"  # stringified

    def test_read_ndjson(self, tmp_path: Path):
        records = [{"x": 1}, {"x": 2}, {"x": 3}]
        path = _write_ndjson(tmp_path, records)
        conn = JsonConnector(SourceConfig(type="json", path=path))
        assert conn.get_raw_columns() == ["x"]
        assert len(conn.read_sample()) == 3

    def test_read_sample_limits_rows(self, tmp_path: Path):
        data = [{"v": i} for i in range(100)]
        path = _write_json(tmp_path, data)
        conn = JsonConnector(SourceConfig(type="json", path=path))
        assert len(conn.read_sample(n=5)) == 5

    def test_read_all_chunks(self, tmp_path: Path):
        data = [{"v": i} for i in range(25)]
        path = _write_json(tmp_path, data)
        conn = JsonConnector(SourceConfig(type="json", path=path))
        chunks = list(conn.read_all(chunk_size=10))
        assert len(chunks) == 3
        assert len(chunks[0]) == 10
        assert len(chunks[1]) == 10
        assert len(chunks[2]) == 5

    def test_get_row_count(self, tmp_path: Path):
        data = [{"a": 1}, {"a": 2}]
        path = _write_json(tmp_path, data)
        conn = JsonConnector(SourceConfig(type="json", path=path))
        assert conn.get_row_count() == 2

    def test_empty_file_raises(self, tmp_path: Path):
        path = tmp_path / "empty.json"
        path.write_text("", encoding="utf-8")
        conn = JsonConnector(SourceConfig(type="json", path=path))
        with pytest.raises(ValueError, match="empty"):
            conn.get_raw_columns()

    def test_missing_file_raises(self, tmp_path: Path):
        conn = JsonConnector(SourceConfig(type="json", path=tmp_path / "nope.json"))
        with pytest.raises(FileNotFoundError):
            conn.get_raw_columns()

    def test_empty_array_raises(self, tmp_path: Path):
        path = _write_json(tmp_path, [])
        conn = JsonConnector(SourceConfig(type="json", path=path))
        with pytest.raises(ValueError, match="no records"):
            conn.get_raw_columns()

    def test_non_object_array_raises(self, tmp_path: Path):
        path = _write_json(tmp_path, [1, 2, 3])
        conn = JsonConnector(SourceConfig(type="json", path=path))
        with pytest.raises(ValueError, match="array of objects"):
            conn.get_raw_columns()

    def test_none_values_become_empty_string(self, tmp_path: Path):
        data = [{"name": "Alice", "email": None}]
        path = _write_json(tmp_path, data)
        conn = JsonConnector(SourceConfig(type="json", path=path))
        rows = conn.read_sample()
        assert rows[0]["email"] == ""

    def test_superset_columns_across_records(self, tmp_path: Path):
        data = [{"a": 1, "b": 2}, {"a": 3, "c": 4}]
        path = _write_json(tmp_path, data)
        conn = JsonConnector(SourceConfig(type="json", path=path))
        cols = conn.get_raw_columns()
        assert "a" in cols
        assert "b" in cols
        assert "c" in cols

    def test_invalid_ndjson_line_raises(self, tmp_path: Path):
        path = tmp_path / "bad.ndjson"
        path.write_text('{"a": 1}\nnot json\n', encoding="utf-8")
        conn = JsonConnector(SourceConfig(type="json", path=path))
        with pytest.raises(ValueError, match="line 2"):
            conn.get_raw_columns()
