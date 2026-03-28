"""Parse LLM responses into structured Pydantic models."""

from __future__ import annotations

import json
import re
from typing import Any

from canopy.models.analysis import FieldMapping, SchemaProposal, SourceAnalysis
from canopy.models.schema import TargetSchema


class ParseError(Exception):
    """Raised when an LLM response cannot be parsed into the expected structure."""


def _extract_json(text: str) -> dict[str, Any]:
    """Extract JSON from an LLM response. Tries code blocks first, then raw."""
    # Try ```json ... ```
    pattern = r"```json\s*\n(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL)
    if matches:
        return json.loads(matches[0])

    # Try raw JSON (find first { ... })
    brace_start = text.find("{")
    brace_end = text.rfind("}")
    if brace_start != -1 and brace_end != -1:
        return json.loads(text[brace_start : brace_end + 1])

    raise ParseError(f"No JSON found in LLM response:\n{text[:200]}")


def parse_source_analysis(text: str) -> SourceAnalysis:
    """Parse LLM response from the 'understand source' step."""
    try:
        data = _extract_json(text)
        return SourceAnalysis(**data)
    except (json.JSONDecodeError, ParseError) as e:
        raise ParseError(f"Failed to parse source analysis: {e}") from e


def parse_mapping_response(
    text: str, target_schema: TargetSchema | None
) -> list[FieldMapping] | SchemaProposal:
    """Parse LLM response from the 'inspect target' step.

    Returns FieldMapping list if target exists, SchemaProposal if target is new.
    """
    try:
        data = _extract_json(text)
    except (json.JSONDecodeError, ParseError) as e:
        raise ParseError(f"Failed to parse mapping response: {e}") from e

    if target_schema is None and "target_schema" in data:
        return SchemaProposal(**data)

    if "field_mappings" in data:
        return [FieldMapping(**m) for m in data["field_mappings"]]

    raise ParseError(f"Unexpected mapping response structure: {list(data.keys())}")


def parse_review_verdict(text: str) -> dict[str, Any]:
    """Parse LLM response from the review step.

    Returns a dict with 'approved' (bool) and optionally 'issues' or 'notes'.
    """
    try:
        data = _extract_json(text)
        if "approved" in data:
            return data
    except (json.JSONDecodeError, ParseError):
        pass

    # If we can't parse JSON, check if the response contains corrected code
    # (which means it wasn't approved)
    if "```python" in text:
        return {"approved": False, "issues": ["LLM provided corrected code"]}

    # Default: assume approved if no clear signal
    return {"approved": True, "notes": "Could not parse review verdict, assuming approved"}
