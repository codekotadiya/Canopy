from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ScriptExecutionResult(BaseModel):
    success: bool
    output_rows: list[dict[str, Any]] = []
    errors: list[str] = []
    row_count_in: int = 0
    row_count_out: int = 0


class LoadSummary(BaseModel):
    rows_loaded: int = 0
    rows_failed: int = 0
    duration_seconds: float = 0.0


class JobSummary(BaseModel):
    job_id: str
    pipeline_name: str
    status: str  # "success", "partial", "failed"
    source_rows: int = 0
    transformed_rows: int = 0
    loaded_rows: int = 0
    failed_rows: int = 0
    script_path: str = ""
    review_iterations: int = 0
    duration_seconds: float = 0.0
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
