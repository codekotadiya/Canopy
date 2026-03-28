"""Integration test for the full agentic pipeline with mocked LLM and SQLite."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from canopy.core.context.engine import ContextEngine
from canopy.models.config import (
    LLMConfig,
    PipelineConfig,
    ScriptConfig,
    SourceConfig,
    TargetConfig,
)


# Canned LLM responses keyed by step detection
UNDERSTAND_RESPONSE = json.dumps(
    {
        "columns": [
            {
                "name": "Full Name",
                "inferred_type": "string",
                "sample_values": ["John Smith", "Jane Doe"],
                "null_count": 1,
                "quality_issues": ["1 empty value"],
            },
            {
                "name": "Email",
                "inferred_type": "email",
                "sample_values": ["john@email.com"],
                "null_count": 1,
                "quality_issues": [],
            },
            {
                "name": "Phone",
                "inferred_type": "phone",
                "sample_values": ["(555) 123-4567"],
                "null_count": 1,
                "quality_issues": ["mixed formats"],
            },
            {
                "name": "Hire Date",
                "inferred_type": "date",
                "sample_values": ["01/15/2020", "2019-03-22"],
                "null_count": 0,
                "quality_issues": ["mixed date formats"],
            },
            {
                "name": "Salary",
                "inferred_type": "currency",
                "sample_values": ["$75,000", "62000"],
                "null_count": 0,
                "quality_issues": ["mixed formats"],
            },
            {
                "name": "Active",
                "inferred_type": "boolean",
                "sample_values": ["Yes", "true", "1"],
                "null_count": 0,
                "quality_issues": ["mixed representations"],
            },
        ],
        "row_count_sample": 5,
        "notes": ["Data has mixed formats"],
    }
)

INSPECT_RESPONSE = json.dumps(
    {
        "target_schema": {
            "table_name": "employees",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False, "primary_key": True},
                {"name": "full_name", "type": "VARCHAR(255)", "nullable": True},
                {"name": "email", "type": "VARCHAR(255)", "nullable": True},
                {"name": "phone", "type": "VARCHAR(50)", "nullable": True},
                {"name": "hire_date", "type": "VARCHAR(20)", "nullable": True},
                {"name": "salary", "type": "FLOAT", "nullable": True},
                {"name": "is_active", "type": "BOOLEAN", "nullable": True},
            ],
        },
        "field_mappings": [
            {
                "source_column": "Full Name",
                "target_column": "full_name",
                "transformation_notes": "strip whitespace",
            },
            {
                "source_column": "Email",
                "target_column": "email",
                "transformation_notes": "lowercase, empty to None",
            },
            {
                "source_column": "Phone",
                "target_column": "phone",
                "transformation_notes": "keep as string",
            },
            {
                "source_column": "Hire Date",
                "target_column": "hire_date",
                "transformation_notes": "keep as string for now",
            },
            {
                "source_column": "Salary",
                "target_column": "salary",
                "transformation_notes": "strip $ and commas, float",
            },
            {
                "source_column": "Active",
                "target_column": "is_active",
                "transformation_notes": "normalize to bool",
            },
        ],
        "rationale": "Standard employee table",
    }
)

GENERATE_RESPONSE = '''```python
def transform(row: dict) -> dict | None:
    full_name = row.get("Full Name", "").strip() or None
    email = row.get("Email", "").strip().lower() or None
    phone = row.get("Phone", "").strip() or None
    hire_date = row.get("Hire Date", "").strip() or None

    salary_raw = row.get("Salary", "").strip()
    salary = None
    if salary_raw:
        salary = float(salary_raw.replace("$", "").replace(",", ""))

    active_raw = row.get("Active", "").strip().lower()
    is_active = active_raw in ("yes", "true", "1")

    return {
        "full_name": full_name,
        "email": email,
        "phone": phone,
        "hire_date": hire_date,
        "salary": salary,
        "is_active": is_active,
    }


def validate(row: dict) -> list[str]:
    warnings = []
    if not row.get("full_name") and not row.get("email"):
        warnings.append("Both name and email are empty")
    return warnings
```'''

REVIEW_APPROVED = json.dumps({"approved": True, "notes": "Output looks correct"})


class FakeLLM:
    """Sequences through canned responses based on call order."""

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


class TestContextEngine:
    def _make_config(self, csv_path: Path, script_dir: Path) -> PipelineConfig:
        return PipelineConfig(
            name="test_pipeline",
            source=SourceConfig(path=csv_path),
            target=TargetConfig(
                type="postgres",
                connection_string="sqlite:///:memory:",
                table_name="employees",
                create_if_missing=True,
            ),
            llm=LLMConfig(provider="ollama"),
            script=ScriptConfig(output_dir=script_dir, max_review_iterations=2),
            chunk_size=10,
        )

    def test_full_pipeline_happy_path(self, sample_csv_path: Path, tmp_path: Path):
        config = self._make_config(sample_csv_path, tmp_path / "scripts")
        engine = ContextEngine(config)

        fake_llm = FakeLLM([
            UNDERSTAND_RESPONSE,
            INSPECT_RESPONSE,
            GENERATE_RESPONSE,
            REVIEW_APPROVED,
        ])
        engine.llm = fake_llm

        logs: list[str] = []
        summary = engine.run(log_fn=logs.append)

        assert summary.status in ("success", "partial")
        assert summary.source_rows == 5
        assert summary.loaded_rows > 0
        assert summary.script_path != ""
        assert Path(summary.script_path).exists()
        assert summary.review_iterations >= 1
        assert len(fake_llm.calls) == 4  # understand, inspect, generate, review

    def test_pipeline_creates_script_file(self, sample_csv_path: Path, tmp_path: Path):
        config = self._make_config(sample_csv_path, tmp_path / "scripts")
        engine = ContextEngine(config)

        engine.llm = FakeLLM([
            UNDERSTAND_RESPONSE,
            INSPECT_RESPONSE,
            GENERATE_RESPONSE,
            REVIEW_APPROVED,
        ])

        summary = engine.run(log_fn=lambda _: None)
        script_path = Path(summary.script_path)
        assert script_path.exists()
        content = script_path.read_text()
        assert "def transform(" in content
        assert "def validate(" in content

    def test_pipeline_review_iteration(self, sample_csv_path: Path, tmp_path: Path):
        """Test that the engine iterates when the review rejects the first script."""
        buggy_script = '''```python
def transform(row: dict) -> dict | None:
    # Buggy: will crash on empty salary
    return {"salary": float(row["Salary"])}
```'''

        config = self._make_config(sample_csv_path, tmp_path / "scripts")
        engine = ContextEngine(config)

        engine.llm = FakeLLM([
            UNDERSTAND_RESPONSE,
            INSPECT_RESPONSE,
            buggy_script,        # first script (will have errors)
            GENERATE_RESPONSE,   # review returns corrected code
            REVIEW_APPROVED,     # second review approves
        ])

        summary = engine.run(log_fn=lambda _: None)
        # Should have iterated at least once
        assert summary.review_iterations >= 1

    def test_pipeline_error_handling(self, tmp_path: Path):
        """Test graceful failure when source file doesn't exist."""
        config = self._make_config(
            tmp_path / "nonexistent.csv", tmp_path / "scripts"
        )
        engine = ContextEngine(config)
        engine.llm = FakeLLM([UNDERSTAND_RESPONSE])

        summary = engine.run(log_fn=lambda _: None)
        assert summary.status == "failed"
        assert len(summary.errors) > 0
