from __future__ import annotations

import csv
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from canopy.models.analysis import (
    ColumnAnalysis,
    FieldMapping,
    SourceAnalysis,
)
from canopy.models.schema import ColumnSchema, TargetSchema


SAMPLE_CSV_ROWS = [
    {
        "Full Name": "John Smith",
        "Email": "john.smith@email.com",
        "Phone": "(555) 123-4567",
        "Hire Date": "01/15/2020",
        "Salary": "$75,000",
        "Active": "Yes",
    },
    {
        "Full Name": "Jane Doe",
        "Email": "jane.doe@gmail.com",
        "Phone": "555.987.6543",
        "Hire Date": "2019-03-22",
        "Salary": "62000",
        "Active": "true",
    },
    {
        "Full Name": "Bob Johnson",
        "Email": "bob.j@company.org",
        "Phone": "5551234567",
        "Hire Date": "March 1, 2021",
        "Salary": "$88,500.00",
        "Active": "1",
    },
    {
        "Full Name": "Alice Williams",
        "Email": "",
        "Phone": "(555) 000-0000",
        "Hire Date": "12/31/2018",
        "Salary": "95000",
        "Active": "No",
    },
    {
        "Full Name": "",
        "Email": "sam@test.com",
        "Phone": "",
        "Hire Date": "2022-06-15",
        "Salary": "$45,000",
        "Active": "false",
    },
]


@pytest.fixture
def sample_csv_path(tmp_path: Path) -> Path:
    """Write a sample CSV with messy data to a temp file."""
    csv_path = tmp_path / "sample.csv"
    fieldnames = list(SAMPLE_CSV_ROWS[0].keys())
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(SAMPLE_CSV_ROWS)
    return csv_path


@pytest.fixture
def db_engine() -> Engine:
    """In-memory SQLite engine for loader tests."""
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def sample_target_schema() -> TargetSchema:
    return TargetSchema(
        table_name="employees",
        columns=[
            ColumnSchema(name="id", type="INTEGER", nullable=False, primary_key=True),
            ColumnSchema(name="full_name", type="VARCHAR(255)"),
            ColumnSchema(name="email", type="VARCHAR(255)"),
            ColumnSchema(name="phone", type="VARCHAR(50)"),
            ColumnSchema(name="hire_date", type="DATE"),
            ColumnSchema(name="salary", type="NUMERIC(12,2)"),
            ColumnSchema(name="is_active", type="BOOLEAN"),
        ],
    )


@pytest.fixture
def sample_source_analysis() -> SourceAnalysis:
    return SourceAnalysis(
        columns=[
            ColumnAnalysis(
                name="Full Name",
                inferred_type="string",
                sample_values=["John Smith", "Jane Doe"],
                null_count=1,
                quality_issues=["1 empty value"],
            ),
            ColumnAnalysis(
                name="Email",
                inferred_type="email",
                sample_values=["john.smith@email.com", "jane.doe@gmail.com"],
                null_count=1,
                quality_issues=["1 empty value"],
            ),
            ColumnAnalysis(
                name="Phone",
                inferred_type="phone",
                sample_values=["(555) 123-4567", "555.987.6543"],
                null_count=1,
                quality_issues=["mixed formats"],
            ),
            ColumnAnalysis(
                name="Hire Date",
                inferred_type="date",
                sample_values=["01/15/2020", "2019-03-22", "March 1, 2021"],
                null_count=0,
                quality_issues=["mixed date formats"],
            ),
            ColumnAnalysis(
                name="Salary",
                inferred_type="currency",
                sample_values=["$75,000", "62000", "$88,500.00"],
                null_count=0,
                quality_issues=["mixed formats: some have $ and commas"],
            ),
            ColumnAnalysis(
                name="Active",
                inferred_type="boolean",
                sample_values=["Yes", "true", "1", "No", "false"],
                null_count=0,
                quality_issues=["mixed boolean representations"],
            ),
        ],
        row_count_sample=5,
        notes=["Data has mixed formats across multiple columns"],
    )


@pytest.fixture
def sample_field_mappings() -> list[FieldMapping]:
    return [
        FieldMapping(
            source_column="Full Name",
            target_column="full_name",
            transformation_notes="Direct mapping, strip whitespace",
        ),
        FieldMapping(
            source_column="Email",
            target_column="email",
            transformation_notes="Lowercase, empty string to None",
        ),
        FieldMapping(
            source_column="Phone",
            target_column="phone",
            transformation_notes="Normalize to digits only",
        ),
        FieldMapping(
            source_column="Hire Date",
            target_column="hire_date",
            transformation_notes="Parse multiple date formats to ISO",
        ),
        FieldMapping(
            source_column="Salary",
            target_column="salary",
            transformation_notes="Strip $ and commas, convert to float",
        ),
        FieldMapping(
            source_column="Active",
            target_column="is_active",
            transformation_notes="Normalize Yes/true/1 to True, No/false/0 to False",
        ),
    ]


class MockLLMProvider:
    """Fake LLM provider that returns canned responses based on prompt content."""

    def __init__(self, responses: dict[str, str] | None = None):
        self.responses = responses or {}
        self.calls: list[str] = []

    def complete(self, prompt: str, system: str | None = None) -> str:
        self.calls.append(prompt)
        for keyword, response in self.responses.items():
            if keyword in prompt:
                return response
        return '{"error": "no matching mock response"}'

    def is_cloud(self) -> bool:
        return False


@pytest.fixture
def mock_llm() -> MockLLMProvider:
    return MockLLMProvider()
