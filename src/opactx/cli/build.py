from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import typer
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from opactx import __version__
from opactx.core.build import (
    BuildError,
    BuildResult,
    SchemaValidationError,
    SourceFetchError,
    build_project,
)

console = Console()

STAGES: list[tuple[str, str]] = [
    ("load_config", "Load config"),
    ("load_intent", "Load intent context"),
    ("fetch_sources", "Fetch sources"),
    ("normalize", "Normalize"),
    ("validate_schema", "Validate schema"),
    ("write_bundle", "Write bundle"),
]


@dataclass
class SourceState:
    name: str
    source_type: str
    status: str = "waiting"
    elapsed: float | None = None
    note: str | None = None


class BuildTui:
    def __init__(self, sources: Iterable[SourceState]):
        self.stage_status = {name: "pending" for name, _ in STAGES}
        self.stage_elapsed: dict[str, float | None] = {}
        self.sources = {state.name: state for state in sources}
        self.is_tty = console.is_terminal
        self._live = None

    def __enter__(self) -> "BuildTui":
        if self.is_tty:
            from rich.live import Live

            self._live = Live(self._render(), console=console, refresh_per_second=10)
            self._live.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._live:
            self._live.__exit__(exc_type, exc, tb)

    def on_stage(self, stage: str, status: str, elapsed: float | None) -> None:
        self.stage_status[stage] = status
        if elapsed is not None:
            self.stage_elapsed[stage] = elapsed
        if self.is_tty and self._live:
            self._live.update(self._render())
        else:
            if status == "start":
                return
            self._print_stage_line(stage, status, elapsed)

    def on_source(
        self, name: str, source_type: str, status: str, elapsed: float | None, note: str | None
    ) -> None:
        state = self.sources.setdefault(
            name, SourceState(name=name, source_type=source_type, note=note)
        )
        state.status = status
        state.elapsed = elapsed
        state.note = note
        if self.is_tty and self._live:
            self._live.update(self._render())
        elif status in {"success", "fail"}:
            console.print(self._format_source_line(state))

    def _print_stage_line(self, stage: str, status: str, elapsed: float | None) -> None:
        index = {name: i for i, (name, _) in enumerate(STAGES, start=1)}.get(stage, 0)
        label = dict(STAGES).get(stage, stage)
        console.print(self._format_stage_line(index, label, status, elapsed))

    def _render(self) -> Group:
        lines = []
        for i, (name, label) in enumerate(STAGES, start=1):
            status = self.stage_status.get(name, "pending")
            elapsed = self.stage_elapsed.get(name)
            lines.append(self._format_stage_line(i, label, status, elapsed))
        renderables = [Text("\n".join(lines))]
        if self.sources:
            renderables.append(self._source_table())
        return Group(*renderables)

    def _format_stage_line(self, index: int, label: str, status: str, elapsed: float | None) -> str:
        glyph, text = _stage_glyph(status)
        suffix = ""
        if status == "start":
            if label == "Fetch sources" and self.sources:
                done = sum(1 for state in self.sources.values() if state.status in {"success", "fail"})
                suffix = f" running ({done}/{len(self.sources)})"
            else:
                suffix = " running"
        duration = _format_duration(elapsed) if elapsed is not None else ""
        duration = f"  {duration}" if duration else ""
        padding = "." * max(2, 28 - len(label))
        return f"[{index}/6] {label} {padding} {glyph} {text}{suffix}{duration}"

    def _source_table(self) -> Table:
        table = Table(title="Sources", show_header=True)
        table.add_column("NAME", style="bold")
        table.add_column("TYPE")
        table.add_column("STATUS")
        table.add_column("TIME", justify="right")
        table.add_column("NOTES")
        for state in self.sources.values():
            glyph, text = _source_glyph(state.status)
            duration = _format_duration(state.elapsed) if state.elapsed is not None else ""
            table.add_row(state.name, state.source_type, f"{glyph} {text}", duration, state.note or "")
        return table

    def _format_source_line(self, state: SourceState) -> str:
        glyph, text = _source_glyph(state.status)
        duration = _format_duration(state.elapsed) if state.elapsed is not None else ""
        note = f" ({state.note})" if state.note else ""
        return f"source {state.name} ({state.source_type}): {glyph} {text} {duration}{note}"


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
    _print_header(project, config)
    sources_state: list[SourceState] = []
    tui = BuildTui(sources_state)

    try:
        with tui:
            result = build_project(
                project,
                config_path=config,
                output_dir=output_dir,
                clean=clean,
                dry_run=dry_run,
                fail_fast=fail_fast,
                on_stage=tui.on_stage,
                on_source=tui.on_source,
            )
    except SourceFetchError as exc:
        _print_source_failure(exc)
        raise typer.Exit(code=2)
    except SchemaValidationError as exc:
        _print_schema_failure(exc)
        raise typer.Exit(code=2)
    except BuildError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=2)
    except Exception as exc:  # noqa: BLE001
        if debug:
            raise
        console.print(f"[red]Unexpected error:[/red] {exc}")
        raise typer.Exit(code=3)

    _print_build_summary(result, dry_run=dry_run)


def _print_header(project: Path, config: Path) -> None:
    console.print(f"opactx v{__version__} | project: {project} | config: {config}")
    console.print("-" * 64)
    console.print("Building policy context bundle (OPA-compatible)")


def _print_source_failure(error: SourceFetchError) -> None:
    body = "\n".join(
        [
            f"source: {error.name} (type={error.source_type})",
            f"error: {error.message}",
            "hint: check credentials; run with --debug for details",
        ]
    )
    console.print(Panel(body, title="Source fetch failed"))


def _print_schema_failure(error: SchemaValidationError) -> None:
    lines = [f"{path}: {message}" for path, message in error.errors[:20]]
    if len(error.errors) > 20:
        lines.append(f"...and {len(error.errors) - 20} more")
    body = "\n".join(
        [
            f"{error.schema_path}",
            "",
            *lines,
            "",
            "Hints:",
            "- Check context/standards.yaml and connector output shape.",
        ]
    )
    console.print(Panel(body, title=f"Schema validation failed ({len(error.errors)} errors)"))


def _print_build_summary(result: BuildResult, *, dry_run: bool) -> None:
    if dry_run:
        console.print("Dry run complete (no files written)")
        console.print(f"Would write: {result.output_dir / 'data.json'}")
        console.print(f"Would write: {result.output_dir / '.manifest'}")
        return

    body = "\n".join(
        [
            f"Path:     {result.output_dir}",
            "Files:    data.json, .manifest",
            f"Revision: {result.revision}",
            f"Schema:   {result.schema_path}",
            f"Sources:  {result.sources_count}",
            "",
            "Next:",
            f"- opactx inspect {result.output_dir}",
        ]
    )
    console.print(Panel(body, title="Build complete"))


def _format_duration(elapsed: float | None) -> str:
    if elapsed is None:
        return ""
    if elapsed < 1:
        return f"{elapsed * 1000:.0f}ms"
    if elapsed < 10:
        return f"{elapsed:.2f}s"
    return f"{elapsed:.1f}s"


def _stage_glyph(status: str) -> tuple[str, str]:
    mapping = {
        "pending": ("⏸", "pending"),
        "start": ("⠋", ""),
        "success": ("✅", ""),
        "fail": ("❌", ""),
        "skip": ("⏭", "skipped"),
    }
    return mapping.get(status, ("?", status))


def _source_glyph(status: str) -> tuple[str, str]:
    mapping = {
        "waiting": ("⏸", "waiting"),
        "start": ("⠋", "fetching"),
        "success": ("✅", "done"),
        "fail": ("❌", "failed"),
    }
    return mapping.get(status, ("?", status))
