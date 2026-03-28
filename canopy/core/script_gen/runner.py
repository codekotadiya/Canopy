from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from canopy.core.script_gen.validator import validate_script
from canopy.models.execution import ScriptExecutionResult

# Maximum time (seconds) a script subprocess is allowed to run.
_DEFAULT_TIMEOUT = 60


def _docker_available() -> bool:
    """Check whether ``docker`` CLI is on PATH and the daemon is reachable."""
    if shutil.which("docker") is None:
        return False
    try:
        proc = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=10,
        )
        return proc.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _build_harness(script_path: str, rows_path: str, output_path: str) -> str:
    """Return a small Python program that loads the generated script, runs
    ``transform()`` on every row read from *rows_path*, and writes results
    plus errors to *output_path* as JSON."""
    return f"""\
import importlib.util, json, sys

def _load():
    spec = importlib.util.spec_from_file_location("canopy_transform", {script_path!r})
    if spec is None or spec.loader is None:
        raise ImportError("Cannot load script")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

rows = json.loads(open({rows_path!r}).read())
output = []
errors = []

try:
    mod = _load()
except Exception as e:
    errors.append(f"Failed to load script: {{e}}")
    json.dump({{"output": output, "errors": errors}}, open({output_path!r}, "w"))
    sys.exit(0)

transform = getattr(mod, "transform", None)
if transform is None:
    errors.append("Script is missing a transform() function")
    json.dump({{"output": output, "errors": errors}}, open({output_path!r}, "w"))
    sys.exit(0)

for i, row in enumerate(rows):
    try:
        result = transform(row)
        if result is not None:
            output.append(result)
    except Exception as e:
        errors.append(f"Row {{i}}: {{type(e).__name__}}: {{e}}")

json.dump({{"output": output, "errors": errors}}, open({output_path!r}, "w"))
"""


class ScriptRunner:
    """Executes generated conversion scripts on data in an isolated subprocess.

    Parameters
    ----------
    timeout:
        Maximum seconds a single execution may run.
    use_docker:
        If ``True``, run the subprocess inside a Docker container for OS-level
        isolation (requires Docker daemon).  If ``None`` (default), auto-detect
        Docker availability and fall back to a plain subprocess.
    docker_image:
        Docker image to use when ``use_docker`` is enabled.  Must have Python 3
        installed.
    """

    def __init__(
        self,
        timeout: int = _DEFAULT_TIMEOUT,
        use_docker: bool | None = None,
        docker_image: str = "python:3.11-slim",
    ) -> None:
        self.timeout = timeout
        self.docker_image = docker_image

        if use_docker is True:
            if not _docker_available():
                raise RuntimeError(
                    "Docker was requested for script isolation but is not available."
                )
            self._use_docker = True
        elif use_docker is False:
            self._use_docker = False
        else:
            # Auto-detect
            self._use_docker = _docker_available()

    def run_on_sample(
        self, script_path: Path, sample_rows: list[dict[str, str]]
    ) -> ScriptExecutionResult:
        """Run the script's transform() on sample rows in a subprocess."""
        return self._run(script_path, sample_rows)

    def run_on_batch(
        self, script_path: Path, rows: list[dict[str, str]]
    ) -> ScriptExecutionResult:
        """Run transform on a batch during full execution."""
        return self._run(script_path, rows)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(
        self, script_path: Path, rows: list[dict[str, str]]
    ) -> ScriptExecutionResult:
        # --- AST validation before execution ---
        try:
            code = script_path.read_text(encoding="utf-8")
        except OSError as exc:
            return ScriptExecutionResult(
                success=False,
                errors=[f"Cannot read script: {exc}"],
                row_count_in=len(rows),
            )

        validation = validate_script(code)
        if not validation.valid:
            return ScriptExecutionResult(
                success=False,
                errors=[f"Script validation failed: {'; '.join(validation.errors)}"],
                row_count_in=len(rows),
            )

        # --- Run in isolated subprocess ---
        rows_path = out_path = harness_path = ""
        try:
            with (
                tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as rows_file,
                tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as out_file,
                tempfile.NamedTemporaryFile(
                    mode="w", suffix=".py", delete=False
                ) as harness_file,
            ):
                rows_path = rows_file.name
                out_path = out_file.name
                harness_path = harness_file.name

                json.dump(rows, rows_file)
                rows_file.flush()

            if self._use_docker:
                harness_code = _build_harness(
                    "/work/script.py", "/work/rows.json", "/work/output.json"
                )
                Path(harness_path).write_text(harness_code, encoding="utf-8")
                proc = subprocess.run(
                    [
                        "docker", "run", "--rm",
                        "--network", "none",
                        "--memory", "256m",
                        "--cpus", "1",
                        "--read-only",
                        "--tmpfs", "/tmp",
                        "-v", f"{script_path}:/work/script.py:ro",
                        "-v", f"{rows_path}:/work/rows.json:ro",
                        "-v", f"{harness_path}:/work/harness.py:ro",
                        "-v", f"{out_path}:/work/output.json",
                        self.docker_image,
                        "python", "/work/harness.py",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=self.timeout,
                )
            else:
                harness_code = _build_harness(
                    str(script_path), rows_path, out_path
                )
                Path(harness_path).write_text(harness_code, encoding="utf-8")
                proc = subprocess.run(
                    [sys.executable, harness_path],
                    capture_output=True,
                    text=True,
                    timeout=self.timeout,
                    # Do not inherit the parent's environment wholesale — pass a
                    # minimal env so the subprocess cannot read secrets etc.
                    env={"PATH": "", "PYTHONPATH": "", "HOME": ""},
                )

            if proc.returncode != 0:
                stderr = proc.stderr[:500] if proc.stderr else "unknown error"
                return ScriptExecutionResult(
                    success=False,
                    errors=[f"Script subprocess failed (rc={proc.returncode}): {stderr}"],
                    row_count_in=len(rows),
                )

            result_data: dict[str, Any] = json.loads(
                Path(out_path).read_text(encoding="utf-8")
            )
            output_rows = result_data.get("output", [])
            errors = result_data.get("errors", [])

            return ScriptExecutionResult(
                success=len(errors) == 0,
                output_rows=output_rows,
                errors=errors,
                row_count_in=len(rows),
                row_count_out=len(output_rows),
            )

        except subprocess.TimeoutExpired:
            return ScriptExecutionResult(
                success=False,
                errors=[f"Script execution timed out after {self.timeout}s"],
                row_count_in=len(rows),
            )
        except Exception as exc:
            return ScriptExecutionResult(
                success=False,
                errors=[f"Runner error: {type(exc).__name__}: {exc}"],
                row_count_in=len(rows),
            )
        finally:
            # Clean up temp files
            for p in (rows_path, out_path, harness_path):
                if p:
                    try:
                        Path(p).unlink(missing_ok=True)
                    except OSError:
                        pass
