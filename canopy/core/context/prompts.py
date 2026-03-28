"""LLM prompt templates for each step of the agentic workflow."""

from __future__ import annotations

import json

from canopy.models.analysis import FieldMapping, SourceAnalysis
from canopy.models.schema import TargetSchema

SYSTEM_PROMPT = (
    "You are Canopy, a data engineering assistant. You analyze source data and generate "
    "Python scripts to transform it into target database schemas. Always respond with "
    "valid JSON when asked for structured output, and valid Python when asked for code. "
    "Be precise about data types and edge cases."
)


def build_understand_source_prompt(
    columns: list[str], sample_rows: list[dict[str, str]]
) -> str:
    rows_json = json.dumps(sample_rows[:20], indent=2)
    return f"""Analyze this dataset sample. Here are the column names:
{json.dumps(columns)}

Here are {min(len(sample_rows), 20)} sample rows as JSON:
{rows_json}

For each column, determine:
1. Semantic meaning (what does this column represent?)
2. Inferred data type: one of "string", "integer", "float", "date", "boolean", "currency", "email", "phone", "json"
3. Up to 5 distinct sample values
4. Count of null/empty values in this sample
5. Data quality issues (mixed formats, special characters, inconsistencies)

Respond ONLY with valid JSON matching this exact structure:
{{
  "columns": [
    {{
      "name": "Original Column Name",
      "inferred_type": "string",
      "sample_values": ["val1", "val2"],
      "null_count": 0,
      "quality_issues": ["issue description"]
    }}
  ],
  "row_count_sample": {len(sample_rows)},
  "notes": ["general observation about the dataset"]
}}"""


def build_inspect_target_prompt(
    source_analysis: SourceAnalysis, target_schema: TargetSchema | None
) -> str:
    analysis_json = source_analysis.model_dump_json(indent=2)

    if target_schema is not None:
        schema_desc = _format_target_schema(target_schema)
        return f"""Here is the analysis of the source data:
{analysis_json}

Here is the existing target database table:
{schema_desc}

Map each source column to the most appropriate target column.
For each mapping, describe what transformation is needed (type casting, formatting, etc.).
Source columns with no target match should be noted as "dropped".
Target columns with no source match should be noted with a suggested default value.

Respond ONLY with valid JSON matching this structure:
{{
  "field_mappings": [
    {{
      "source_column": "Original Name",
      "target_column": "target_name",
      "transformation_notes": "description of needed transformation"
    }}
  ]
}}"""
    else:
        return f"""Here is the analysis of the source data:
{analysis_json}

No target table exists yet. Propose a PostgreSQL table schema that best represents
this data. Use appropriate SQL types (VARCHAR, INTEGER, NUMERIC, TIMESTAMP, BOOLEAN,
TEXT, DATE, etc.). Include a primary key if one can be inferred from the data,
otherwise add a SERIAL id column.

Also provide the field mappings from source columns to target columns.

Respond ONLY with valid JSON matching this structure:
{{
  "target_schema": {{
    "table_name": "proposed_table_name",
    "columns": [
      {{
        "name": "column_name",
        "type": "SQL_TYPE",
        "nullable": true,
        "primary_key": false,
        "default": null
      }}
    ]
  }},
  "field_mappings": [
    {{
      "source_column": "Original Name",
      "target_column": "target_name",
      "transformation_notes": "description"
    }}
  ],
  "rationale": "Brief explanation of schema design choices"
}}"""


def build_generate_script_prompt(
    source_analysis: SourceAnalysis,
    field_mappings: list[FieldMapping],
    target_schema: TargetSchema,
) -> str:
    analysis_json = source_analysis.model_dump_json(indent=2)
    mappings_json = json.dumps([m.model_dump() for m in field_mappings], indent=2)
    schema_json = target_schema.model_dump_json(indent=2)

    return f"""Write Python functions to transform source data rows to the target schema.

Source data analysis:
{analysis_json}

Field mappings (source -> target):
{mappings_json}

Target schema:
{schema_json}

Requirements:
- Write a `def transform(row: dict) -> dict | None:` function
  - Input `row` is a dict where ALL values are strings (from CSV reader)
  - Output dict must have keys matching target column names exactly
  - Handle type conversions: parse dates, strip currency symbols, convert booleans, etc.
  - Handle null/empty values: use None for nullable fields
  - Handle edge cases noted in the source analysis (mixed formats, etc.)
  - Return None to skip/filter invalid rows
- Write a `def validate(row: dict) -> list[str]:` function
  - Input is a transformed row
  - Return a list of warning strings (empty list if OK)
  - Check for: null required fields, out-of-range values, suspicious patterns
- Do NOT import external packages. Only use: datetime, decimal, re, json (already imported)
- Use 4-space indentation

Respond with ONLY the Python code inside a ```python code block.
Do not include import statements (they are provided by the template)."""


def build_review_output_prompt(
    script_code: str,
    sample_input: list[dict[str, str]],
    sample_output: list[dict],
    errors: list[str],
) -> str:
    input_json = json.dumps(sample_input[:5], indent=2)
    output_json = json.dumps(sample_output[:5], indent=2, default=str)
    errors_str = "\n".join(errors) if errors else "None"

    return f"""Review the output of a data conversion script.

The script:
```python
{script_code}
```

Sample input rows (first 5):
{input_json}

Sample output rows (first 5):
{output_json}

Errors encountered:
{errors_str}

Check for:
1. Are output types correct for the target schema?
2. Are there data quality issues (wrong dates, truncated values, lost precision)?
3. Are edge cases handled (nulls, empty strings, unexpected formats)?
4. Did any rows error that shouldn't have, or vice versa?

If the output is correct and complete, respond with:
```json
{{"approved": true, "notes": "brief assessment"}}
```

If issues exist, respond with the CORRECTED Python code in a ```python block,
followed by:
```json
{{"approved": false, "issues": ["issue1", "issue2"]}}
```"""


def _format_target_schema(schema: TargetSchema) -> str:
    lines = [f"Table: {schema.table_name}"]
    for col in schema.columns:
        pk = " [PK]" if col.primary_key else ""
        null = " NOT NULL" if not col.nullable else ""
        default = f" DEFAULT {col.default}" if col.default else ""
        lines.append(f"  {col.name} {col.type}{pk}{null}{default}")
    return "\n".join(lines)
