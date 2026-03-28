"""Canopy CLI — command-line interface for running normalization pipelines."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from canopy.config.loader import load_config
from canopy.core.context.engine import ContextEngine
from canopy.core.script_gen.runner import ScriptRunner
from canopy.models.execution import JobSummary

app = typer.Typer(
    name="canopy",
    help="AI-powered data normalization pipeline",
    no_args_is_help=True,
)
console = Console()


def _print_summary(summary: JobSummary) -> None:
    table = Table(title=f"Pipeline: {summary.pipeline_name}", show_header=False)
    table.add_column("Metric", style="bold")
    table.add_column("Value")

    table.add_row("Job ID", summary.job_id)
    table.add_row("Status", summary.status)
    table.add_row("Source Rows", str(summary.source_rows))
    table.add_row("Transformed", str(summary.transformed_rows))
    table.add_row("Loaded", str(summary.loaded_rows))
    table.add_row("Failed", str(summary.failed_rows))
    table.add_row("Script", summary.script_path)
    table.add_row("Review Iterations", str(summary.review_iterations))
    table.add_row("Duration", f"{summary.duration_seconds:.1f}s")

    console.print(table)

    if summary.warnings:
        console.print("\n[yellow]Warnings:[/yellow]")
        for w in summary.warnings:
            console.print(f"  - {w}")

    if summary.errors:
        console.print("\n[red]Errors:[/red]")
        for e in summary.errors[:20]:
            console.print(f"  - {e}")


@app.command()
def run(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML config"),
) -> None:
    """Run the full agentic normalization pipeline."""
    config = load_config(config_path)
    engine = ContextEngine(config)
    summary = engine.run(log_fn=lambda msg: console.print(msg))
    _print_summary(summary)

    if summary.status == "failed":
        raise typer.Exit(code=1)


@app.command()
def validate(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML config"),
) -> None:
    """Validate a pipeline YAML config without running it."""
    try:
        config = load_config(config_path)
        console.print(f"[green]Config valid:[/green] {config.name}")
        console.print(f"  Source: {config.source.type} ({config.source.path})")
        console.print(f"  Target: {config.target.type} ({config.target.table_name})")
        console.print(f"  LLM: {config.llm.provider} ({config.llm.model})")
    except Exception as e:
        console.print(f"[red]Config invalid:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def rerun(
    script_path: str = typer.Argument(..., help="Path to a previously generated script"),
    config_path: str = typer.Argument(..., help="Path to pipeline YAML config"),
) -> None:
    """Re-execute an existing conversion script without LLM involvement."""
    config = load_config(config_path)
    script = Path(script_path)
    if not script.exists():
        console.print(f"[red]Script not found:[/red] {script}")
        raise typer.Exit(code=1)

    from canopy.core.context.factories import create_connector, create_loader

    connector = create_connector(config)
    loader = create_loader(config)
    runner = ScriptRunner()

    total_loaded = 0
    total_failed = 0
    total_source = 0

    console.print(f"Re-running script: {script}")
    for chunk in connector.read_all(chunk_size=config.chunk_size):
        total_source += len(chunk)
        result = runner.run_on_batch(script, chunk)
        if result.output_rows:
            loaded = loader.load_batch(config.target.table_name, result.output_rows)
            total_loaded += loaded
        total_failed += len(result.errors)

    load_summary = loader.finalize()
    console.print(
        f"Done: {total_loaded} loaded, {total_failed} failed "
        f"from {total_source} source rows"
    )


if __name__ == "__main__":
    app()
