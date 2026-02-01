from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from opactx.cli.renderers import RunOpaPlainRenderer, RunOpaRichRenderer, run_events
from opactx.core.run_opa import run_opa_events

console = Console()


def run_opa(
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
    bundle: Path | None = typer.Option(
        None,
        "--bundle",
        help="Bundle directory to use (defaults to output.dir).",
    ),
    policy: Path | None = typer.Option(
        None,
        "--policy",
        help="Policy directory to load (defaults to ./policy).",
    ),
    address: str = typer.Option(
        "http://localhost:8181",
        "--addr",
        help="OPA server address.",
    ),
    skip_build: bool = typer.Option(
        False,
        "--skip-build",
        help="Use existing bundle without rebuilding.",
    ),
) -> None:
    events = run_opa_events(
        project_dir=project,
        config_path=config,
        bundle_path=bundle,
        policy_path=policy,
        address=address,
        skip_build=skip_build,
    )
    renderer = RunOpaRichRenderer(console) if console.is_terminal else RunOpaPlainRenderer(console)
    exit_code = run_events(events, renderer)
    raise typer.Exit(code=exit_code)
