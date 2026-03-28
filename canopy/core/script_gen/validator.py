"""AST-based validation for LLM-generated scripts.

Parses generated code and rejects dangerous constructs before execution.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field

# Modules that generated scripts are allowed to import.
# Extend this set if your pipelines genuinely need more.
ALLOWED_MODULES: frozenset[str] = frozenset(
    {
        "csv",
        "re",
        "sys",
        "math",
        "decimal",
        "datetime",
        "json",
        "hashlib",
        "string",
        "unicodedata",
        "collections",
        "itertools",
        "functools",
        "typing",
    }
)

# Built-in functions that must never appear in generated scripts.
BLOCKED_BUILTINS: frozenset[str] = frozenset(
    {
        "eval",
        "exec",
        "compile",
        "__import__",
        "globals",
        "locals",
        "breakpoint",
        "exit",
        "quit",
        "input",
        "memoryview",
        "help",
    }
)

# Attribute names that indicate dangerous operations.
BLOCKED_ATTRIBUTES: frozenset[str] = frozenset(
    {
        "__subclasses__",
        "__bases__",
        "__globals__",
        "__code__",
        "__builtins__",
        "__import__",
        "system",
        "popen",
        "exec",
        "eval",
    }
)


@dataclass
class ValidationResult:
    """Result of AST validation."""

    valid: bool
    errors: list[str] = field(default_factory=list)


def validate_script(code: str) -> ValidationResult:
    """Validate generated script code via AST analysis.

    Checks for:
    - Syntax correctness
    - Presence of ``transform(row)`` function
    - Blocked imports (os, subprocess, socket, etc.)
    - Blocked builtins (eval, exec, open, etc.)
    - Blocked attribute access patterns
    """
    errors: list[str] = []

    # --- Parse ---
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return ValidationResult(valid=False, errors=[f"Syntax error: {exc}"])

    # --- Check for transform() function ---
    has_transform = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "transform":
            args = node.args
            # Must accept at least one positional arg (the row)
            if len(args.args) < 1:
                errors.append(
                    "transform() must accept at least one argument (row)"
                )
            has_transform = True
            break

    if not has_transform:
        errors.append("Script must define a transform(row) function")

    # --- Walk the AST for dangerous constructs ---
    for node in ast.walk(tree):
        # Check imports
        if isinstance(node, ast.Import):
            for alias in node.names:
                _check_module(alias.name, errors)

        elif isinstance(node, ast.ImportFrom):
            if node.module:
                _check_module(node.module, errors)

        # Check calls to blocked builtins
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in BLOCKED_BUILTINS:
                errors.append(f"Blocked builtin call: {func.id}()")
            elif isinstance(func, ast.Attribute) and func.attr in BLOCKED_ATTRIBUTES:
                errors.append(f"Blocked attribute access: .{func.attr}")

        # Check bare attribute access (not just in calls)
        elif isinstance(node, ast.Attribute):
            if node.attr in BLOCKED_ATTRIBUTES:
                errors.append(f"Blocked attribute access: .{node.attr}")

    return ValidationResult(valid=len(errors) == 0, errors=errors)


def _check_module(module_name: str, errors: list[str]) -> None:
    """Verify a module import is on the allow-list."""
    top_level = module_name.split(".")[0]
    if top_level not in ALLOWED_MODULES:
        errors.append(f"Blocked import: {module_name} (allowed: {', '.join(sorted(ALLOWED_MODULES))})")
