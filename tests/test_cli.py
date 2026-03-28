from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from canopy.triggers.cli import app

runner = CliRunner()


class TestCLI:
    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "AI-powered" in result.output

    def test_validate_valid_config(self, tmp_path: Path):
        config = tmp_path / "pipeline.yaml"
        config.write_text(
            """
name: test
source:
  path: ./data.csv
target:
  connection_string: postgresql://u:p@host/db
  table_name: out
""",
            encoding="utf-8",
        )
        result = runner.invoke(app, ["validate", str(config)])
        assert result.exit_code == 0
        assert "Config valid" in result.output

    def test_validate_invalid_config(self, tmp_path: Path):
        config = tmp_path / "bad.yaml"
        config.write_text("not: valid: config:", encoding="utf-8")
        result = runner.invoke(app, ["validate", str(config)])
        assert result.exit_code == 1

    def test_validate_missing_config(self):
        result = runner.invoke(app, ["validate", "/nonexistent/config.yaml"])
        assert result.exit_code == 1

    def test_run_subcommand_exists(self):
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        assert "config" in result.output.lower()

    def test_rerun_subcommand_exists(self):
        result = runner.invoke(app, ["rerun", "--help"])
        assert result.exit_code == 0
