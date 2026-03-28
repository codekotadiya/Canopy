"""Factory functions to instantiate pipeline components from config."""

from __future__ import annotations

from canopy.core.ingestion.base import BaseConnector
from canopy.core.loader.base import BaseLoader
from canopy.llm.base import BaseLLMProvider
from canopy.models.config import PipelineConfig


def create_connector(config: PipelineConfig) -> BaseConnector:
    source_type = config.source.type.lower()
    if source_type == "csv":
        from canopy.core.ingestion.csv_connector import CsvConnector

        return CsvConnector(config.source)
    raise ValueError(f"Unsupported source type: {source_type}")


def create_llm_provider(config: PipelineConfig) -> BaseLLMProvider:
    provider = config.llm.provider.lower()
    if provider == "ollama":
        from canopy.llm.ollama import OllamaProvider

        return OllamaProvider(config.llm)
    raise ValueError(f"Unsupported LLM provider: {provider}")


def create_loader(config: PipelineConfig) -> BaseLoader:
    target_type = config.target.type.lower()
    if target_type in ("postgres", "postgresql"):
        from canopy.core.loader.postgres import PostgresLoader

        return PostgresLoader(config.target.connection_string)
    raise ValueError(f"Unsupported target type: {target_type}")
