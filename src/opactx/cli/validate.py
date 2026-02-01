from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from opactx.cli.renderers import (
    ValidateJsonRenderer,
    ValidatePlainRenderer,
    ValidateRichRenderer,
    run_events,
)
from opactx.core.validate import validate_events

console = Console()


def validate(
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
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Require that referenced plugins resolve.",
    ),
    check_schema: bool = typer.Option(
        True,
        "--check-schema/--no-check-schema",
        help="Validate a candidate context against the schema.",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Show stack traces for unexpected errors.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Emit machine-readable JSON report.",
    ),
) -> None:
    events = validate_events(
        project_dir=project,
        config_path=config,
        strict=strict,
        check_schema=check_schema,
        debug=debug,
    )
    if json_output:
        renderer = ValidateJsonRenderer(console)
    else:
        renderer = ValidateRichRenderer(console) if console.is_terminal else ValidatePlainRenderer(console)
    exit_code = run_events(events, renderer)
    raise typer.Exit(code=exit_code)
