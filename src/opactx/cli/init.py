from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from opactx.cli.renderers import InitPlainRenderer, InitRichRenderer, run_events
from opactx.core.init import init_events

console = Console()


def init(
    project: Path = typer.Argument(
        Path("."),
        help="Directory to initialize (created if missing).",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite existing scaffold files.",
    ),
    minimal: bool = typer.Option(
        False,
        "--minimal",
        help="Generate only the minimal required files to run opactx build.",
    ),
    with_examples: bool = typer.Option(
        False,
        "--with-examples",
        help="Include example fixtures and sample policy module(s).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Do not write files; print planned operations.",
    ),
    name: str | None = typer.Option(
        None,
        "--name",
        help="Optional project name for README title.",
    ),
    no_policy: bool = typer.Option(
        False,
        "--no-policy",
        help="Do not scaffold policy/ directory.",
    ),
    schema: str = typer.Option(
        "jsonschema",
        "--schema",
        help="Contract schema type (jsonschema or openapi).",
    ),
) -> None:
    events = init_events(
        project=project,
        force=force,
        minimal=minimal,
        with_examples=with_examples,
        dry_run=dry_run,
        name=name,
        no_policy=no_policy,
        schema=schema,
    )
    renderer = InitRichRenderer(console) if console.is_terminal else InitPlainRenderer(console)
    exit_code = run_events(events, renderer)
    raise typer.Exit(code=exit_code)
