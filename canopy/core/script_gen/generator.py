from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from canopy.core.script_gen.template import DEFAULT_VALIDATE_FUNC, SCRIPT_TEMPLATE


def extract_python_code(llm_response: str) -> str:
    """Extract Python code from an LLM response.

    Looks for ```python ... ``` code blocks first, falls back to the entire response.
    """
    pattern = r"```python\s*\n(.*?)```"
    matches = re.findall(pattern, llm_response, re.DOTALL)
    if matches:
        return matches[0].strip()
    # Fallback: try generic code blocks
    pattern = r"```\s*\n(.*?)```"
    matches = re.findall(pattern, llm_response, re.DOTALL)
    if matches:
        return matches[0].strip()
    # Last resort: return the raw response
    return llm_response.strip()


def _split_functions(code: str) -> tuple[str, str]:
    """Split LLM-generated code into transform and validate functions.

    Returns (transform_func, validate_func). If validate is missing,
    uses the default.
    """
    # Check if code contains both functions
    if "def transform(" in code and "def validate(" in code:
        # Split at "def validate"
        parts = code.split("def validate(", 1)
        transform_func = parts[0].rstrip()
        validate_func = "def validate(" + parts[1]
        return transform_func, validate_func

    if "def transform(" in code:
        return code, DEFAULT_VALIDATE_FUNC

    # If the code doesn't have def transform, wrap it
    return f"def transform(row: dict) -> dict | None:\n{_indent(code)}", DEFAULT_VALIDATE_FUNC


def _indent(code: str, spaces: int = 4) -> str:
    """Indent each line of code by the given number of spaces."""
    prefix = " " * spaces
    return "\n".join(prefix + line if line.strip() else line for line in code.splitlines())


class ScriptGenerator:
    """Generates and saves conversion scripts from LLM output."""

    def save_script(
        self,
        code: str,
        job_id: str,
        output_dir: Path,
        source_description: str = "",
        target_table: str = "",
    ) -> Path:
        """Save a generated script to disk. Returns the path to the saved file."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{job_id}_convert.py"
        filepath = output_dir / filename

        transform_func, validate_func = _split_functions(code)

        content = SCRIPT_TEMPLATE.format(
            timestamp=datetime.now(timezone.utc).isoformat(),
            job_id=job_id,
            source_description=source_description.replace("\\", "/"),
            target_table=target_table,
            filename=filename,
            transform_func=transform_func,
            validate_func=validate_func,
        )

        filepath.write_text(content, encoding="utf-8")
        return filepath
