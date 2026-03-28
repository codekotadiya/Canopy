from __future__ import annotations

from pathlib import Path

from canopy.core.script_gen.runner import ScriptRunner


def _write_script(tmp_path: Path, code: str, name: str = "test_convert.py") -> Path:
    path = tmp_path / name
    path.write_text(code, encoding="utf-8")
    return path


class TestScriptRunner:
    def test_successful_transform(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            '''
def transform(row):
    return {"name": row["Full Name"].strip(), "active": row["Active"].lower() == "yes"}

def validate(row):
    return []
''',
        )
        runner = ScriptRunner()
        result = runner.run_on_sample(
            script,
            [{"Full Name": "John Smith", "Active": "Yes"}],
        )
        assert result.success is True
        assert len(result.output_rows) == 1
        assert result.output_rows[0]["name"] == "John Smith"
        assert result.output_rows[0]["active"] is True
        assert result.errors == []

    def test_transform_with_errors(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            '''
def transform(row):
    return {"value": int(row["amount"])}
''',
        )
        runner = ScriptRunner()
        result = runner.run_on_sample(
            script,
            [{"amount": "100"}, {"amount": "bad"}, {"amount": "200"}],
        )
        assert result.success is False
        assert result.row_count_in == 3
        assert result.row_count_out == 2
        assert len(result.errors) == 1
        assert "Row 1" in result.errors[0]

    def test_transform_returns_none_filters_row(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            '''
def transform(row):
    if row["skip"] == "yes":
        return None
    return row
''',
        )
        runner = ScriptRunner()
        result = runner.run_on_sample(
            script,
            [{"skip": "no", "v": "1"}, {"skip": "yes", "v": "2"}, {"skip": "no", "v": "3"}],
        )
        assert result.success is True
        assert result.row_count_out == 2

    def test_missing_transform_function(self, tmp_path: Path):
        script = _write_script(tmp_path, "x = 1\n")
        runner = ScriptRunner()
        result = runner.run_on_sample(script, [{"a": "1"}])
        assert result.success is False
        assert "transform" in result.errors[0].lower()

    def test_script_load_failure(self, tmp_path: Path):
        script = tmp_path / "nonexistent.py"
        runner = ScriptRunner()
        result = runner.run_on_sample(script, [{"a": "1"}])
        assert result.success is False
        assert "Cannot read script" in result.errors[0]

    def test_syntax_error_in_script(self, tmp_path: Path):
        script = _write_script(tmp_path, "def transform(row\n    return row\n")
        runner = ScriptRunner()
        result = runner.run_on_sample(script, [{"a": "1"}])
        assert result.success is False
        assert "Syntax error" in result.errors[0]

    def test_blocked_import_rejected(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            'import os\ndef transform(row):\n    return row\n',
        )
        runner = ScriptRunner()
        result = runner.run_on_sample(script, [{"a": "1"}])
        assert result.success is False
        assert "Blocked import" in result.errors[0]

    def test_blocked_builtin_rejected(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            'def transform(row):\n    exec("pass")\n    return row\n',
        )
        runner = ScriptRunner()
        result = runner.run_on_sample(script, [{"a": "1"}])
        assert result.success is False
        assert "Blocked builtin" in result.errors[0]
