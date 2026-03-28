"""Tests for failure modes across the pipeline.

Covers: unapproved script blocking, review loop exhaustion, loader row-level
fallback, empty CSV handling, AST validation edge cases, and loader exception
handling in the engine.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from canopy.core.context.engine import ContextEngine
from canopy.core.context.parsers import parse_review_verdict
from canopy.core.ingestion.csv_connector import CsvConnector
from canopy.core.loader.postgres import PostgresLoader
from canopy.core.script_gen.validator import validate_script
from canopy.models.config import (
    LLMConfig,
    PipelineConfig,
    ScriptConfig,
    SourceConfig,
    TargetConfig,
)
from canopy.models.schema import ColumnSchema, TargetSchema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

UNDERSTAND_RESPONSE = json.dumps(
    {
        "columns": [
            {
                "name": "Name",
                "inferred_type": "string",
                "sample_values": ["Alice"],
                "null_count": 0,
                "quality_issues": [],
            }
        ],
        "row_count_sample": 1,
        "notes": [],
    }
)

INSPECT_RESPONSE = json.dumps(
    {
        "target_schema": {
            "table_name": "t",
            "columns": [
                {"name": "name", "type": "VARCHAR(100)", "nullable": True},
            ],
        },
        "field_mappings": [
            {
                "source_column": "Name",
                "target_column": "name",
                "transformation_notes": "direct",
            }
        ],
    }
)

GOOD_SCRIPT = '''```python
def transform(row: dict) -> dict | None:
    return {"name": row.get("Name", "").strip() or None}
```'''

REVIEW_APPROVED = json.dumps({"approved": True, "notes": "Looks good"})
REVIEW_REJECTED = json.dumps({"approved": False, "issues": ["bad output"]})


class FakeLLM:
    def __init__(self, responses: list[str]):
        self.responses = responses
        self.call_index = 0
        self.calls: list[str] = []

    def complete(self, prompt: str, system: str | None = None) -> str:
        self.calls.append(prompt)
        resp = self.responses[min(self.call_index, len(self.responses) - 1)]
        self.call_index += 1
        return resp

    def is_cloud(self) -> bool:
        return False

    def health_check(self) -> bool:
        return True


def _make_config(csv_path: Path, script_dir: Path) -> PipelineConfig:
    return PipelineConfig(
        name="test",
        source=SourceConfig(path=csv_path),
        target=TargetConfig(
            type="sqlite",
            connection_string="sqlite:///:memory:",
            table_name="t",
            create_if_missing=True,
        ),
        llm=LLMConfig(provider="ollama"),
        script=ScriptConfig(output_dir=script_dir, max_review_iterations=2),
        chunk_size=10,
    )


def _write_csv(tmp_path: Path, content: str = "Name\nAlice\nBob\n") -> Path:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(content, encoding="utf-8")
    return csv_path


# ---------------------------------------------------------------------------
# 1. Unapproved script blocks full execution
# ---------------------------------------------------------------------------

class TestUnapprovedScriptBlocking:
    def test_unapproved_script_fails_job(self, tmp_path: Path):
        csv_path = _write_csv(tmp_path)
        config = _make_config(csv_path, tmp_path / "scripts")
        engine = ContextEngine(config)
        # Review always rejects, no revised code → should fail
        engine.llm = FakeLLM([
            UNDERSTAND_RESPONSE,
            INSPECT_RESPONSE,
            GOOD_SCRIPT,
            REVIEW_REJECTED,
        ])
        summary = engine.run(log_fn=lambda _: None)
        assert summary.status == "failed"
        assert any("not approved" in e.lower() for e in summary.errors)

    def test_max_review_iterations_exhausted(self, tmp_path: Path):
        csv_path = _write_csv(tmp_path)
        config = _make_config(csv_path, tmp_path / "scripts")
        engine = ContextEngine(config)
        # Review always rejects but provides new code each time
        revision1 = '''```python
def transform(row: dict) -> dict | None:
    return {"name": row.get("Name", "")}
```'''
        revision2 = '''```python
def transform(row: dict) -> dict | None:
    return {"name": row.get("Name", "").upper()}
```'''
        engine.llm = FakeLLM([
            UNDERSTAND_RESPONSE,
            INSPECT_RESPONSE,
            GOOD_SCRIPT,
            revision1,    # review 1 rejects with new code
            revision2,    # review 2 rejects with new code (max_review_iterations=2)
        ])
        summary = engine.run(log_fn=lambda _: None)
        assert summary.status == "failed"
        assert any("not approved" in e.lower() for e in summary.errors)


# ---------------------------------------------------------------------------
# 2. Review parser fail-closed
# ---------------------------------------------------------------------------

class TestReviewParserFailClosed:
    def test_malformed_json_defaults_to_rejected(self):
        verdict = parse_review_verdict("This is not JSON at all")
        assert verdict["approved"] is False

    def test_empty_response_defaults_to_rejected(self):
        verdict = parse_review_verdict("")
        assert verdict["approved"] is False

    def test_json_without_approved_field_defaults_to_rejected(self):
        verdict = parse_review_verdict('{"notes": "looks fine"}')
        assert verdict["approved"] is False

    def test_python_code_in_response_means_rejected(self):
        verdict = parse_review_verdict("Here is the fix:\n```python\ndef transform(row):\n    return row\n```")
        assert verdict["approved"] is False

    def test_valid_approval_is_accepted(self):
        verdict = parse_review_verdict('{"approved": true, "notes": "ok"}')
        assert verdict["approved"] is True


# ---------------------------------------------------------------------------
# 3. CSV empty file handling
# ---------------------------------------------------------------------------

class TestCsvEmptyFile:
    def test_empty_csv_raises_on_get_columns(self, tmp_path: Path):
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("", encoding="utf-8")
        connector = CsvConnector(SourceConfig(path=csv_path))
        with pytest.raises(ValueError, match="empty"):
            connector.get_raw_columns()

    def test_header_only_csv_returns_empty_sample(self, tmp_path: Path):
        csv_path = tmp_path / "header_only.csv"
        csv_path.write_text("Name,Age\n", encoding="utf-8")
        connector = CsvConnector(SourceConfig(path=csv_path))
        assert connector.get_raw_columns() == ["Name", "Age"]
        assert connector.read_sample() == []


# ---------------------------------------------------------------------------
# 4. Loader row-level fallback
# ---------------------------------------------------------------------------

class TestLoaderRowFallback:
    def test_batch_with_bad_row_partially_succeeds(self):
        loader = PostgresLoader("sqlite:///:memory:")
        schema = TargetSchema(
            table_name="strict_t",
            columns=[
                ColumnSchema(name="id", type="INTEGER", nullable=False, primary_key=True),
                ColumnSchema(name="val", type="VARCHAR(50)"),
            ],
        )
        loader.ensure_table(schema)

        # Two good rows and one duplicate PK → should load 2, quarantine 1
        rows = [
            {"id": 1, "val": "a"},
            {"id": 2, "val": "b"},
            {"id": 1, "val": "duplicate"},  # PK conflict
        ]
        loaded = loader.load_batch("strict_t", rows)
        # The batch insert will fail (PK conflict), then row-by-row:
        # row 1 ok, row 2 ok, row 3 fails
        assert loaded == 2
        assert loader._rows_failed == 1
        assert len(loader._quarantined) == 1
        loader.finalize()

    def test_all_bad_rows_returns_zero(self):
        loader = PostgresLoader("sqlite:///:memory:")
        schema = TargetSchema(
            table_name="strict_t2",
            columns=[
                ColumnSchema(name="id", type="INTEGER", nullable=False, primary_key=True),
            ],
        )
        loader.ensure_table(schema)

        # Insert first row
        loader.load_batch("strict_t2", [{"id": 1}])

        # All duplicates
        loaded = loader.load_batch("strict_t2", [{"id": 1}, {"id": 1}])
        assert loaded == 0
        assert loader._rows_failed == 2
        loader.finalize()


# ---------------------------------------------------------------------------
# 5. AST validation edge cases
# ---------------------------------------------------------------------------

class TestASTValidationEdgeCases:
    def test_nested_dangerous_import(self):
        code = "def transform(row):\n    import subprocess\n    return row\n"
        result = validate_script(code)
        assert not result.valid
        assert any("subprocess" in e for e in result.errors)

    def test_from_import_blocked(self):
        code = "from os.path import join\ndef transform(row):\n    return row\n"
        result = validate_script(code)
        assert not result.valid
        assert any("os" in e for e in result.errors)

    def test_dunder_attribute_access(self):
        code = 'def transform(row):\n    x = "".__class__.__bases__\n    return row\n'
        result = validate_script(code)
        assert not result.valid
        assert any("__bases__" in e for e in result.errors)

    def test_globals_call_blocked(self):
        code = "def transform(row):\n    g = globals()\n    return row\n"
        result = validate_script(code)
        assert not result.valid
        assert any("globals" in e for e in result.errors)

    def test_compile_blocked(self):
        code = 'def transform(row):\n    compile("1+1", "<>", "eval")\n    return row\n'
        result = validate_script(code)
        assert not result.valid
        assert any("compile" in e for e in result.errors)

    def test_allowed_imports_pass(self):
        code = (
            "import re\nimport json\nfrom datetime import datetime\n"
            "def transform(row):\n    return row\n"
        )
        result = validate_script(code)
        assert result.valid

    def test_transform_with_no_args_rejected(self):
        code = "def transform():\n    return {}\n"
        result = validate_script(code)
        assert not result.valid
        assert any("argument" in e for e in result.errors)

    def test_multiple_violations_reported(self):
        code = "import os\nimport subprocess\ndef transform(row):\n    exec('x')\n    return row\n"
        result = validate_script(code)
        assert not result.valid
        assert len(result.errors) >= 3  # os, subprocess, exec


# ---------------------------------------------------------------------------
# 6. Engine handles loader exceptions per chunk
# ---------------------------------------------------------------------------

class TestEngineLoaderExceptionHandling:
    def test_loader_exception_does_not_crash_pipeline(self, tmp_path: Path):
        csv_path = _write_csv(tmp_path, "Name\nAlice\nBob\nCharlie\n")
        config = _make_config(csv_path, tmp_path / "scripts")
        config = config.model_copy(update={"chunk_size": 1})  # one row per chunk
        engine = ContextEngine(config)
        engine.llm = FakeLLM([
            UNDERSTAND_RESPONSE,
            INSPECT_RESPONSE,
            GOOD_SCRIPT,
            REVIEW_APPROVED,
        ])

        # Sabotage the loader to fail on the second call
        original_load_batch = engine.loader.load_batch
        call_count = 0

        def failing_load_batch(table_name, rows):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("Simulated DB failure")
            return original_load_batch(table_name, rows)

        engine.loader.load_batch = failing_load_batch

        summary = engine.run(log_fn=lambda _: None)
        # Should not be fully failed — partial success
        assert summary.status in ("partial", "success")
        assert summary.loaded_rows > 0
        assert any("Loader error" in e for e in summary.errors)
