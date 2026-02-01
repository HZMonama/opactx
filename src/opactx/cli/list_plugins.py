from __future__ import annotations

import typer
from rich.console import Console

from opactx.cli.renderers import (
    ListPluginsJsonRenderer,
    ListPluginsPlainRenderer,
    ListPluginsRichRenderer,
    run_events,
)
from opactx.core.list_plugins import list_plugins_events

console = Console()


def list_plugins(
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Emit machine-readable JSON report.",
    ),
) -> None:
    events = list_plugins_events()
    if json_output:
        renderer = ListPluginsJsonRenderer(console)
    else:
        renderer = ListPluginsRichRenderer(console) if console.is_terminal else ListPluginsPlainRenderer(console)
    exit_code = run_events(events, renderer)
    raise typer.Exit(code=exit_code)
