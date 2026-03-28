from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    type: str = "csv"
    path: Path
    delimiter: str = ","
    encoding: str = "utf-8"
    sample_size: int = Field(default=50, ge=1, le=1000)


class TargetConfig(BaseModel):
    type: str = "postgres"
    connection_string: str
    table_name: str
    create_if_missing: bool = True


class LLMConfig(BaseModel):
    provider: str = "ollama"
    model: str = "llama3"
    base_url: str = "http://localhost:11434"
    temperature: float = Field(default=0.1, ge=0.0, le=2.0)
    timeout: int = Field(default=120, ge=1)


class ScriptConfig(BaseModel):
    output_dir: Path = Path("scripts")
    max_review_iterations: int = Field(default=3, ge=1, le=10)


class PipelineConfig(BaseModel):
    name: str
    source: SourceConfig
    target: TargetConfig
    llm: LLMConfig = LLMConfig()
    script: ScriptConfig = ScriptConfig()
    chunk_size: int = Field(default=1000, ge=1)
