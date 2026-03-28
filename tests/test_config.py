from __future__ import annotations

from pathlib import Path

import pytest

from canopy.config.loader import load_config
from canopy.models.config import PipelineConfig


def _write_config(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "pipeline.yaml"
    p.write_text(content, encoding="utf-8")
    return p


class TestLoadConfig:
    def test_load_valid_config(self, tmp_path: Path):
        config_path = _write_config(
            tmp_path,
            """
name: test_pipeline
source:
  type: csv
  path: ./data.csv
target:
  type: postgres
  connection_string: postgresql://user:pass@localhost/db
  table_name: output
""",
        )
        config = load_config(config_path)
        assert isinstance(config, PipelineConfig)
        assert config.name == "test_pipeline"
        assert config.source.type == "csv"
        assert config.target.table_name == "output"

    def test_defaults_applied(self, tmp_path: Path):
        config_path = _write_config(
            tmp_path,
            """
name: defaults_test
source:
  path: ./data.csv
target:
  connection_string: postgresql://user:pass@localhost/db
  table_name: out
""",
        )
        config = load_config(config_path)
        assert config.source.delimiter == ","
        assert config.source.encoding == "utf-8"
        assert config.source.sample_size == 50
        assert config.llm.provider == "ollama"
        assert config.llm.model == "llama3"
        assert config.script.max_review_iterations == 3
        assert config.chunk_size == 1000

    def test_env_var_interpolation(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("TEST_DB_URL", "postgresql://u:p@host/db")
        config_path = _write_config(
            tmp_path,
            """
name: env_test
source:
  path: ./data.csv
target:
  connection_string: ${TEST_DB_URL}
  table_name: out
""",
        )
        config = load_config(config_path)
        assert config.target.connection_string == "postgresql://u:p@host/db"

    def test_missing_env_var_raises(self, tmp_path: Path):
        config_path = _write_config(
            tmp_path,
            """
name: missing_env
source:
  path: ./data.csv
target:
  connection_string: ${NONEXISTENT_VAR_12345}
  table_name: out
""",
        )
        with pytest.raises(ValueError, match="NONEXISTENT_VAR_12345"):
            load_config(config_path)

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.yaml")

    def test_invalid_yaml_raises(self, tmp_path: Path):
        config_path = _write_config(tmp_path, "- - - not: valid: yaml: [")
        with pytest.raises(Exception):
            load_config(config_path)

    def test_missing_required_fields_raises(self, tmp_path: Path):
        config_path = _write_config(
            tmp_path,
            """
name: incomplete
source:
  type: csv
""",
        )
        with pytest.raises(Exception):
            load_config(config_path)
