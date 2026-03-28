from __future__ import annotations

from pydantic import BaseModel

from canopy.models.schema import TargetSchema


class ColumnAnalysis(BaseModel):
    name: str
    inferred_type: str  # e.g. "string", "integer", "float", "date", "boolean", "currency"
    sample_values: list[str] = []
    null_count: int = 0
    quality_issues: list[str] = []


class SourceAnalysis(BaseModel):
    columns: list[ColumnAnalysis]
    row_count_sample: int
    notes: list[str] = []


class FieldMapping(BaseModel):
    source_column: str
    target_column: str
    transformation_notes: str = ""


class SchemaProposal(BaseModel):
    target_schema: TargetSchema
    field_mappings: list[FieldMapping]
    rationale: str = ""
