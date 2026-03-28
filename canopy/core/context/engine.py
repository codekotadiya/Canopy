"""Agentic context engine — the core orchestrator for Canopy pipelines."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

from canopy.core.context.factories import create_connector, create_llm_provider, create_loader
from canopy.core.context.parsers import (
    parse_mapping_response,
    parse_review_verdict,
    parse_source_analysis,
)
from canopy.core.context.prompts import (
    SYSTEM_PROMPT,
    build_generate_script_prompt,
    build_inspect_target_prompt,
    build_review_output_prompt,
    build_understand_source_prompt,
)
from canopy.core.script_gen.generator import ScriptGenerator, extract_python_code
from canopy.core.script_gen.runner import ScriptRunner
from canopy.core.script_gen.validator import validate_script
from canopy.models.analysis import FieldMapping, SchemaProposal
from canopy.models.config import PipelineConfig
from canopy.models.execution import JobSummary


class CanopyError(Exception):
    """Base exception for pipeline errors."""


class ContextEngine:
    """Orchestrates the agentic pipeline workflow."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.connector = create_connector(config)
        self.llm = create_llm_provider(config)
        self.loader = create_loader(config)
        self.generator = ScriptGenerator()
        self.runner = ScriptRunner()

    def run(self, log_fn=print) -> JobSummary:
        """Execute the full agentic pipeline. Returns a structured summary."""
        start_time = time.monotonic()
        job_id = uuid.uuid4().hex[:12]
        warnings: list[str] = []
        errors: list[str] = []

        try:
            # Provider health check
            if hasattr(self.llm, "health_check") and not self.llm.health_check():
                raise CanopyError(
                    f"LLM provider health check failed. "
                    f"Ensure the provider ({self.config.llm.provider}) is running and reachable."
                )

            # Privacy warning
            if self.llm.is_cloud():
                msg = (
                    "WARNING: Using a cloud LLM provider. Sample data will be sent to "
                    f"{self.config.llm.provider} servers for analysis."
                )
                log_fn(msg)
                warnings.append(msg)

            # === Step 1: Understand Source ===
            log_fn("[Step 1/6] Reading source data sample...")
            columns = self.connector.get_raw_columns()
            sample_rows = self.connector.read_sample(self.config.source.sample_size)
            log_fn(f"  Read {len(sample_rows)} sample rows with {len(columns)} columns")

            log_fn("[Step 2/6] Analyzing source data with LLM...")
            understand_prompt = build_understand_source_prompt(columns, sample_rows)
            analysis_text = self.llm.complete(understand_prompt, system=SYSTEM_PROMPT)
            source_analysis = parse_source_analysis(analysis_text)
            log_fn(f"  Identified {len(source_analysis.columns)} columns")

            # === Step 2: Inspect Target ===
            log_fn("[Step 3/6] Inspecting target schema...")
            target_schema = self.loader.get_target_schema(self.config.target.table_name)

            if target_schema is None and not self.config.target.create_if_missing:
                raise CanopyError(
                    f"Target table '{self.config.target.table_name}' does not exist "
                    "and create_if_missing=False"
                )

            if target_schema is not None:
                log_fn(f"  Found existing table with {len(target_schema.columns)} columns")
            else:
                log_fn("  Table does not exist, will propose new schema")

            inspect_prompt = build_inspect_target_prompt(source_analysis, target_schema)
            mapping_text = self.llm.complete(inspect_prompt, system=SYSTEM_PROMPT)
            mapping_result = parse_mapping_response(mapping_text, target_schema)

            field_mappings: list[FieldMapping]
            if isinstance(mapping_result, SchemaProposal):
                target_schema = mapping_result.target_schema
                field_mappings = mapping_result.field_mappings
                self.loader.ensure_table(target_schema)
                log_fn(f"  Created new table: {target_schema.table_name}")
            else:
                field_mappings = mapping_result
                if target_schema is None:
                    raise CanopyError("No target schema available")

            log_fn(f"  Mapped {len(field_mappings)} fields")

            # === Step 3: Generate Script ===
            log_fn("[Step 4/6] Generating conversion script...")
            gen_prompt = build_generate_script_prompt(
                source_analysis, field_mappings, target_schema
            )
            script_text = self.llm.complete(gen_prompt, system=SYSTEM_PROMPT)
            script_code = extract_python_code(script_text)

            # Validate the raw LLM-generated code before saving
            validation = validate_script(script_code)
            if not validation.valid:
                raise CanopyError(
                    f"Generated script failed AST validation: "
                    f"{'; '.join(validation.errors)}"
                )

            output_dir = Path(self.config.script.output_dir)
            script_path = self.generator.save_script(
                script_code,
                job_id,
                output_dir,
                source_description=str(self.config.source.path),
                target_table=self.config.target.table_name,
            )
            log_fn(f"  Script saved to: {script_path}")

            # === Steps 4-5: Review Loop ===
            review_iterations = 0
            script_approved = False
            for iteration in range(self.config.script.max_review_iterations):
                review_iterations = iteration + 1
                log_fn(f"[Step 5/6] Review iteration {review_iterations}...")

                result = self.runner.run_on_sample(script_path, sample_rows)

                review_prompt = build_review_output_prompt(
                    script_code, sample_rows, result.output_rows, result.errors
                )
                review_text = self.llm.complete(review_prompt, system=SYSTEM_PROMPT)
                verdict = parse_review_verdict(review_text)

                if verdict.get("approved", False) and result.success:
                    script_approved = True
                    log_fn(f"  Script approved after {review_iterations} iteration(s)")
                    break

                # Script needs revision
                issues = verdict.get("issues", result.errors)
                log_fn(f"  Revising script: {issues}")

                revised_code = extract_python_code(review_text)
                if revised_code != script_code:
                    script_code = revised_code
                    script_path = self.generator.save_script(
                        script_code,
                        job_id,
                        output_dir,
                        source_description=str(self.config.source.path),
                        target_table=self.config.target.table_name,
                    )
                else:
                    warnings.append(
                        f"Review iteration {review_iterations}: LLM did not provide revised code"
                    )
                    break
            else:
                warnings.append(
                    f"Max review iterations ({self.config.script.max_review_iterations}) reached"
                )

            if not script_approved:
                raise CanopyError(
                    "Script was not approved after review. "
                    "Refusing to execute unapproved script on full dataset."
                )

            # === Step 6: Full Execution ===
            log_fn("[Step 6/6] Executing on full dataset...")
            total_source = 0
            total_transformed = 0
            total_loaded = 0
            total_failed = 0

            for chunk in self.connector.read_all(chunk_size=self.config.chunk_size):
                total_source += len(chunk)
                result = self.runner.run_on_batch(script_path, chunk)
                total_transformed += result.row_count_out
                if result.errors:
                    total_failed += len(result.errors)
                    errors.extend(result.errors[:10])  # cap error logging

                if result.output_rows:
                    try:
                        loaded = self.loader.load_batch(
                            self.config.target.table_name, result.output_rows
                        )
                        total_loaded += loaded
                    except Exception as load_exc:
                        load_err = f"Loader error: {type(load_exc).__name__}: {load_exc}"
                        errors.append(load_err)
                        total_failed += len(result.output_rows)

            self.loader.finalize()
            duration = time.monotonic() - start_time

            status = "success"
            if total_failed > 0:
                status = "partial"
            if total_loaded == 0 and total_source > 0:
                status = "failed"

            log_fn(
                f"  Done: {total_loaded} loaded, {total_failed} failed, "
                f"{duration:.1f}s total"
            )

            return JobSummary(
                job_id=job_id,
                pipeline_name=self.config.name,
                status=status,
                source_rows=total_source,
                transformed_rows=total_transformed,
                loaded_rows=total_loaded,
                failed_rows=total_failed,
                script_path=str(script_path),
                review_iterations=review_iterations,
                duration_seconds=round(duration, 2),
                warnings=warnings,
                errors=errors,
            )

        except Exception as e:
            duration = time.monotonic() - start_time
            return JobSummary(
                job_id=job_id,
                pipeline_name=self.config.name,
                status="failed",
                duration_seconds=round(duration, 2),
                errors=[f"{type(e).__name__}: {e}"],
                warnings=warnings,
            )
