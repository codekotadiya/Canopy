from __future__ import annotations

import py_compile
from pathlib import Path

from canopy.core.script_gen.generator import ScriptGenerator, extract_python_code


class TestExtractPythonCode:
    def test_extracts_from_python_block(self):
        response = 'Here is the code:\n```python\ndef transform(row):\n    return row\n```\nDone.'
        assert extract_python_code(response) == "def transform(row):\n    return row"

    def test_extracts_from_generic_block(self):
        response = 'Code:\n```\ndef transform(row):\n    pass\n```'
        assert extract_python_code(response) == "def transform(row):\n    pass"

    def test_fallback_to_raw_response(self):
        response = "def transform(row):\n    return row"
        assert extract_python_code(response) == response


class TestScriptGenerator:
    def test_save_script_creates_file(self, tmp_path: Path):
        gen = ScriptGenerator()
        code = 'def transform(row: dict) -> dict:\n    return {"name": row["Full Name"]}'
        path = gen.save_script(code, "abc123", tmp_path, "test.csv", "employees")
        assert path.exists()
        assert path.name == "abc123_convert.py"

    def test_saved_script_is_valid_python(self, tmp_path: Path):
        gen = ScriptGenerator()
        code = (
            'def transform(row: dict) -> dict | None:\n'
            '    return {"name": row.get("Full Name", "")}\n\n\n'
            'def validate(row: dict) -> list[str]:\n'
            '    warnings = []\n'
            '    if not row.get("name"):\n'
            '        warnings.append("name is empty")\n'
            '    return warnings'
        )
        path = gen.save_script(code, "valid_test", tmp_path)
        py_compile.compile(str(path), doraise=True)

    def test_saved_script_has_default_validate(self, tmp_path: Path):
        gen = ScriptGenerator()
        code = 'def transform(row: dict) -> dict:\n    return row'
        path = gen.save_script(code, "no_validate", tmp_path)
        content = path.read_text()
        assert "def validate(" in content

    def test_creates_output_dir(self, tmp_path: Path):
        gen = ScriptGenerator()
        code = 'def transform(row: dict) -> dict:\n    return row'
        nested = tmp_path / "a" / "b" / "c"
        path = gen.save_script(code, "nested", nested)
        assert path.exists()
