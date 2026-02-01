from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from opactx.cli.renderers import BuildPlainRenderer, BuildRichRenderer, run_events
from opactx.core.build import build_events

console = Console()


def build(
    config: Path = typer.Option(
        Path("opactx.yaml"),
        "--config",
        "-c",
        help="Path to opactx.yaml.",
    ),
    project: Path = typer.Option(
        Path("."),
        "--project",
        "-p",
        help="Base directory for relative paths.",
    ),
    output_dir: Path | None = typer.Option(
        None,
        "--output-dir",
        help="Override output.dir from config.",
    ),
    clean: bool = typer.Option(
        False,
        "--clean",
        help="Delete existing output dir before writing.",
    ),
    fail_fast: bool = typer.Option(
        True,
        "--fail-fast/--no-fail-fast",
        help="Stop on first error.",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Show stack traces for unexpected errors.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Do everything except writing output files.",
    ),
) -> None:
    events = build_events(
        project_dir=project,
        config_path=config,
        output_dir=output_dir,
        clean=clean,
        dry_run=dry_run,
        fail_fast=fail_fast,
        debug=debug,
    )

    renderer = BuildRichRenderer(console) if console.is_terminal else BuildPlainRenderer(console)
    exit_code = run_events(events, renderer)
    raise typer.Exit(code=exit_code)
