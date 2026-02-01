from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from opactx.cli.renderers import (
    InspectJsonRenderer,
    InspectPlainRenderer,
    InspectRichRenderer,
    run_events,
)
from opactx.core.inspect import inspect_events

console = Console()


def inspect(
    bundle: Path = typer.Argument(
        Path("."),
        help="Bundle directory containing data.json and .manifest.",
    ),
    pointer: str | None = typer.Option(
        None,
        "--path",
        help="JSON pointer to extract from data.json.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Emit machine-readable JSON report.",
    ),
) -> None:
    events = inspect_events(bundle_path=bundle, pointer=pointer)
    if json_output:
        renderer = InspectJsonRenderer(console)
    else:
        renderer = InspectRichRenderer(console) if console.is_terminal else InspectPlainRenderer(console)
    exit_code = run_events(events, renderer)
    raise typer.Exit(code=exit_code)
