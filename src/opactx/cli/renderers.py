from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from opactx import __version__
from opactx.core import events as ev
from opactx.core.stages import (
    BUILD_STAGES,
    INIT_STAGES,
    INSPECT_STAGES,
    VALIDATE_STAGES,
)

RULE_WIDTH = 64
RULE_LINE = "-" * RULE_WIDTH
STATUS_GLYPHS = {
    "pending": "â¸",
    "running": "â ‹",
    "success": "âœ…",
    "failed": "âŒ",
    "skipped": "â­",
    "partial": "âš ï¸",
    "warning": "âš ï¸",
}

VALIDATE_CHECK_LABELS = {
    "load_config": "Config",
    "load_schema": "Schema",
    "load_intent": "Intent",
    "resolve_plugins": "Plugins",
    "schema_check": "Schema check",
}


def run_events(events: Iterable[ev.OpactxEvent], renderer: "Renderer") -> int:
    exit_code = 0
    for event in events:
        renderer.handle(event)
        if isinstance(event, ev.CommandCompleted):
            exit_code = event.exit_code
    renderer.close()
    return exit_code


class Renderer:
    def handle(self, event: ev.OpactxEvent) -> None:  # noqa: D401
        """Handle a single event."""

    def close(self) -> None:
        return None


@dataclass
class SourceState:
    name: str
    source_type: str
    status: str = "waiting"
    elapsed_ms: float | None = None
    note: str | None = None
    size_bytes: int | None = None


class BuildRichRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self.is_tty = console.is_terminal
        self.stage_status = {name: "pending" for name, _ in BUILD_STAGES}
        self.stage_elapsed: dict[str, float] = {}
        self.stage_progress: dict[str, tuple[int, int]] = {}
        self.sources: dict[str, SourceState] = {}
        self._live: Live | None = None
        self._schema_path: Path | None = None
        self._bundle_info: dict[str, str] | None = None
        self._dry_run = False
        self._output_dir: Path | None = None
        self._source_failure: ev.SourceFetchFailed | None = None
        self._schema_failure: ev.SchemaValidationFailed | None = None
        self._stage_failure: ev.StageFailed | None = None
        self._show_sources = False
        self._bundle_failure: ev.BundleWriteFailed | None = None

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            self._dry_run = bool(event.options and event.options.get("dry_run"))
            self._print_header(event)
            if self.is_tty:
                self._live = Live(self._render(), console=self.console, refresh_per_second=10)
                self._live.__enter__()
            return
        if isinstance(event, ev.StageStarted):
            self.stage_status[event.stage_id] = "running"
            if event.stage_id == "fetch_sources":
                self._show_sources = True
            self._refresh()
            return
        if isinstance(event, ev.StageCompleted):
            self.stage_status[event.stage_id] = event.status
            self.stage_elapsed[event.stage_id] = event.duration_ms
            self._refresh()
            return
        if isinstance(event, ev.StageFailed):
            self.stage_status[event.stage_id] = "failed"
            self.stage_elapsed[event.stage_id] = event.duration_ms
            self._stage_failure = event
            self._refresh()
            return
        if isinstance(event, ev.StageProgress):
            self.stage_progress[event.stage_id] = (event.current, event.total)
            self._refresh()
            return
        if isinstance(event, ev.SchemaLoaded):
            self._schema_path = Path(event.path) if event.path else None
            return
        if isinstance(event, ev.SourcesPlanned):
            for info in event.sources:
                name = info.get("name", "")
                source_type = info.get("type", "")
                note = info.get("notes")
                if not name:
                    continue
                self.sources[name] = SourceState(
                    name=name,
                    source_type=source_type,
                    status="waiting",
                    note=note,
                )
            self._refresh()
            return
        if isinstance(event, ev.SchemaValidationFailed):
            self._schema_failure = event
            return
        if isinstance(event, ev.SourceFetchStarted):
            state = self.sources.get(event.name)
            if state:
                state.status = "fetching"
                state.note = event.notes or state.note
            else:
                self.sources[event.name] = SourceState(
                    name=event.name,
                    source_type=event.type_key,
                    status="fetching",
                    note=event.notes,
                )
            self._refresh()
            return
        if isinstance(event, ev.SourceFetchCompleted):
            state = self.sources.get(event.name)
            if state:
                state.status = "done"
                state.elapsed_ms = event.duration_ms
                state.size_bytes = event.size_bytes
            self._refresh()
            return
        if isinstance(event, ev.SourceFetchFailed):
            state = self.sources.get(event.name)
            if state:
                state.status = "failed"
                state.elapsed_ms = event.duration_ms
            self._source_failure = event
            self._refresh()
            return
        if isinstance(event, ev.BundleWriteStarted):
            self._output_dir = Path(event.out_dir) if event.out_dir else None
            return
        if isinstance(event, ev.BundleWriteFailed):
            self._bundle_failure = event
            return
        if isinstance(event, ev.BundleWritten):
            self._bundle_info = {
                "out_dir": str(event.out_dir),
                "revision": event.revision,
                "files": ", ".join(event.files),
            }
            return
        if isinstance(event, ev.CommandCompleted):
            self._finish(event)

    def close(self) -> None:
        if self._live:
            self._live.__exit__(None, None, None)
            self._live = None

    def _finish(self, event: ev.CommandCompleted) -> None:
        if self._live:
            self._live.__exit__(None, None, None)
            self._live = None
        if not event.ok:
            if self._source_failure:
                self.console.print(_source_failure_panel(self._source_failure))
            elif self._schema_failure:
                self.console.print(_schema_failure_panel(self._schema_failure))
            elif self._bundle_failure:
                body = "\n".join(
                    [
                        f"out_dir: {self._bundle_failure.out_dir}",
                        f"error: {_redact(self._bundle_failure.message)}",
                    ]
                )
                self.console.print(Panel(body, title="Bundle write failed", box=box.ROUNDED, title_align="left"))
            elif self._stage_failure:
                self.console.print(_stage_failure_panel(self._stage_failure))
            return
        if self._dry_run:
            self.console.print("Dry run complete (no files written)")
            if self._output_dir:
                self.console.print(f"Would write: {self._output_dir}/{{data.json,.manifest}}")
            return
        if self._bundle_info:
            self.console.print("[green]âœ… Build complete[/green]")
            next_lines = [f"- opactx inspect {self._bundle_info['out_dir']}"]
            if Path("policy").exists():
                next_lines.append(f"- conftest test --bundle {self._bundle_info['out_dir']} ./policy")
            body = "\n".join(
                [
                    f"Path:     {self._bundle_info['out_dir']}",
                    f"Files:    {self._bundle_info['files']}",
                    f"Revision: {self._bundle_info['revision']}",
                    f"Schema:   {self._schema_path}",
                    f"Sources:  {len(self.sources)}",
                    "",
                    "Next:",
                    *next_lines,
                ]
            )
            self.console.print(Panel(body, title="Bundle output", box=box.ROUNDED, title_align="left"))

    def _print_header(self, event: ev.CommandStarted) -> None:
        _print_header(self.console, event)
        self.console.print("Building policy context bundle (OPA-compatible)")

    def _refresh(self) -> None:
        if self._live:
            self._live.update(self._render())

    def _render(self) -> Group:
        total = len(BUILD_STAGES)
        stage_table = Table(show_header=True, box=box.MINIMAL, show_lines=False)
        stage_table.add_column("#", justify="right", style="dim")
        stage_table.add_column("Stage")
        stage_table.add_column("Status")
        stage_table.add_column("Time", justify="right")
        for index, (stage_id, label) in enumerate(BUILD_STAGES, start=1):
            status = self.stage_status.get(stage_id, "pending")
            elapsed = self.stage_elapsed.get(stage_id)
            progress = self.stage_progress.get(stage_id)
            status_text = _format_status_text(status, progress)
            if stage_id == "write_bundle" and status == "skipped" and self._dry_run:
                status_text = f"{status_text} (--dry-run)"
            duration = _format_duration(elapsed) if elapsed is not None else ""
            stage_table.add_row(f"{index}/{total}", label, status_text, duration)
        renderables: list = [Panel(stage_table, title="Stages", box=box.ROUNDED, title_align="left")]
        if self._show_sources and self.sources:
            sources_table = _sources_table(self.sources.values(), title=None)
            renderables.append(Panel(sources_table, title="Sources", box=box.ROUNDED, title_align="left"))
        return Group(*renderables)


class BuildPlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._schema_path: Path | None = None
        self._output_dir: Path | None = None
        self._bundle_info: dict[str, str] | None = None
        self._dry_run = False
        self._source_failure: ev.SourceFetchFailed | None = None
        self._schema_failure: ev.SchemaValidationFailed | None = None
        self._stage_failure: ev.StageFailed | None = None
        self._sources_count = 0
        self._source_notes: dict[str, str] = {}
        self._source_types: dict[str, str] = {}
        self._sources_total = 0
        self._bundle_failure: ev.BundleWriteFailed | None = None

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            self._dry_run = bool(event.options and event.options.get("dry_run"))
            _print_header(self.console, event)
            self.console.print("Building policy context bundle (OPA-compatible)")
            return
        if isinstance(event, ev.StageStarted):
            label = _stage_label(event.stage_id, BUILD_STAGES)
            index = _stage_index(event.stage_id, BUILD_STAGES)
            note = None
            if event.stage_id == "fetch_sources" and self._sources_total:
                note = f"({self._sources_total} sources)"
            line = _format_stage_start_line(index, label, len(BUILD_STAGES), note)
            self.console.print(line)
            return
        if isinstance(event, ev.StageCompleted):
            label = _stage_label(event.stage_id, BUILD_STAGES)
            index = _stage_index(event.stage_id, BUILD_STAGES)
            note = None
            if event.stage_id == "write_bundle" and event.status == "skipped" and self._dry_run:
                note = "(--dry-run)"
            line = _format_stage_line(
                index,
                label,
                event.status,
                event.duration_ms,
                None,
                note,
                len(BUILD_STAGES),
                True,
            )
            self.console.print(line)
            return
        if isinstance(event, ev.StageFailed):
            label = _stage_label(event.stage_id, BUILD_STAGES)
            index = _stage_index(event.stage_id, BUILD_STAGES)
            line = _format_stage_line(
                index,
                label,
                "failed",
                event.duration_ms,
                None,
                None,
                len(BUILD_STAGES),
                True,
            )
            details = []
            if event.message:
                details.append(f"FAIL: {_redact(event.message)}")
            if event.hint:
                details.append(f"HINT: {_redact(event.hint)}")
            if details:
                line = f"{line}\n" + "\n".join(details)
            self.console.print(line)
            self._stage_failure = event
            return
        if isinstance(event, ev.SourcesPlanned):
            self._sources_total = len(event.sources)
            for info in event.sources:
                name = info.get("name", "")
                if name:
                    self._source_types[name] = info.get("type", "")
                notes = info.get("notes")
                if name and notes:
                    self._source_notes[name] = notes
            return
        if isinstance(event, ev.SchemaLoaded):
            self._schema_path = Path(event.path) if event.path else None
            if event.path:
                self.console.print(f"Schema: {event.path}")
            return
        if isinstance(event, ev.SchemaValidationFailed):
            self._schema_failure = event
            return
        if isinstance(event, ev.SourceFetchStarted):
            source_type = self._source_types.get(event.name, event.type_key or "")
            self.console.print(f"SOURCE START {event.name} ({source_type})")
            if event.notes:
                self._source_notes[event.name] = event.notes
            return
        if isinstance(event, ev.SourceFetchCompleted):
            self._sources_count += 1
            size = ""
            if event.size_bytes is not None:
                size = f" {_format_bytes(event.size_bytes)}"
            note = self._source_notes.get(event.name)
            note_suffix = f" ({_redact(note)})" if note else ""
            duration = _format_duration(event.duration_ms)
            self.console.print(f"SOURCE OK {event.name} {duration}{size}{note_suffix}")
            return
        if isinstance(event, ev.SourceFetchFailed):
            self._sources_count += 1
            self._source_failure = event
            message = _redact(event.message)
            line = f"SOURCE FAIL {event.name}: {message}"
            if event.hint:
                line = f"{line}\nHINT: {_redact(event.hint)}"
            self.console.print(line)
            return
        if isinstance(event, ev.BundleWriteStarted):
            self._output_dir = Path(event.out_dir) if event.out_dir else None
            if self._output_dir:
                self.console.print(f"Writing bundle to {self._output_dir}")
            return
        if isinstance(event, ev.BundleWriteFailed):
            self._bundle_failure = event
            self.console.print(f"BUNDLE FAIL: {_redact(event.message)}")
            return
        if isinstance(event, ev.BundleWritten):
            self._bundle_info = {
                "out_dir": str(event.out_dir),
                "revision": event.revision,
                "files": ", ".join(event.files),
            }
            return
        if isinstance(event, ev.CommandCompleted):
            self._finish(event)

    def _finish(self, event: ev.CommandCompleted) -> None:
        if not event.ok:
            if self._source_failure:
                self.console.print("Source fetch failed")
                self.console.print(f"source: {self._source_failure.name} (type={self._source_failure.type_key})")
                self.console.print(f"error: {_redact(self._source_failure.message)}")
                return
            if self._schema_failure:
                self.console.print("Schema validation failed")
                for item in self._schema_failure.errors[:20]:
                    self.console.print(f"{item['path']}: {_redact(item['message'])}")
                return
            if self._stage_failure:
                self.console.print(f"Error: {_redact(self._stage_failure.message)}")
                return
            return
        if self._dry_run:
            self.console.print("Dry run complete (no files written)")
            if self._output_dir:
                self.console.print(f"Would write: {self._output_dir}/{{data.json,.manifest}}")
            return
        if self._bundle_info:
            self.console.print(
                f"BUNDLE OK {self._bundle_info['out_dir']} revision={self._bundle_info['revision']}"
            )


class InitRichRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._project: Path | None = None
        self._warnings: list[str] = []
        self._failed: ev.StageFailed | None = None
        self._plan_started = False
        self._dry_run = False
        self._planned: list[tuple[str, Path, str]] = []

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            self._project = Path(event.project_dir) if event.project_dir else None
            self._dry_run = bool(event.options and event.options.get("dry_run"))
            _print_header(self.console, event)
            return
        if isinstance(event, ev.StageStarted) and event.stage_id == "plan_scaffold":
            project = self._project or Path(".")
            self.console.print(f"Initializing opactx project at {project}\n{RULE_LINE}")
            self._plan_started = True
            return
        if isinstance(event, ev.FilePlanned):
            if event.path:
                destination = Path(event.path)
                note = "new" if event.op == "CREATE" else "exists" if event.op == "SKIP" else ""
                self._planned.append((event.op, destination, note))
            return
        if isinstance(event, ev.StageStarted) and event.stage_id == "apply_scaffold":
            if not self._dry_run:
                self.console.print("Writing files...")
            return
        if isinstance(event, ev.FileWritten):
            return
        if isinstance(event, ev.FileWriteFailed):
            message = _redact(event.message)
            body = "\n".join(
                [
                    f"path: {event.path}",
                    f"error: {message}",
                ]
            )
            self.console.print(Panel(body, title="Write failed", box=box.ROUNDED, title_align="left"))
            return
        if isinstance(event, ev.Warning):
            self._warnings.append(_redact(event.message))
            return
        if isinstance(event, ev.StageFailed):
            self._failed = event
            return
        if isinstance(event, ev.StageCompleted) and event.stage_id == "plan_scaffold":
            if not self._plan_started:
                project = self._project or Path(".")
                self.console.print(f"Initializing opactx project at {project}\n{RULE_LINE}")
            plan_table = Table(show_header=True, box=box.MINIMAL)
            plan_table.add_column("Action", style="bold")
            plan_table.add_column("Path")
            plan_table.add_column("State", style="dim")
            for action, path, note in self._planned:
                plan_table.add_row(_scaffold_action_text(action), str(path), note)
            self.console.print("Scaffold")
            self.console.print(RULE_LINE)
            self.console.print(plan_table)
            return
        if isinstance(event, ev.CommandCompleted):
            if event.ok:
                if self._dry_run:
                    self.console.print("\nDry run (no files written).")
                else:
                    self._print_summary()
            else:
                self._print_error()

    def _print_summary(self) -> None:
        project = self._project or Path(".")
        self.console.print("\n[green]Project initialized[/green]")
        self.console.print(RULE_LINE)
        self.console.print(f"- Location: {project}")
        self.console.print("- Editable files:")
        for line in _editable_scaffold_lines(project):
            self.console.print(f"  - {line}")
        self.console.print("")
        self.console.print("- Next: run `opactx validate`, then `opactx build`.")
        for warning in self._warnings:
            self.console.print(f"[yellow]Warning:[/yellow] {warning}")

    def _print_error(self) -> None:
        if not self._failed:
            self.console.print("[red]Initialization failed.[/red]")
            return
        body_lines = [
            f"stage: {self._failed.stage_id}",
            f"error: {_redact(self._failed.message)}",
        ]
        if self._failed.hint:
            body_lines.append(f"hint: {_redact(self._failed.hint)}")
        body = "\n".join(body_lines)
        self.console.print(Panel(body, title="Initialization failed", box=box.ROUNDED, title_align="left"))


class InitPlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._warnings: list[str] = []
        self._project: Path | None = None
        self._failed: ev.StageFailed | None = None
        self._dry_run = False

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            self._project = Path(event.project_dir) if event.project_dir else None
            self._dry_run = bool(event.options and event.options.get("dry_run"))
            _print_header(self.console, event)
            return
        if isinstance(event, ev.StageStarted) and event.stage_id == "plan_scaffold":
            project = self._project or Path(".")
            self.console.print(f"Initializing opactx project at {project}\n{RULE_LINE}")
            return
        if isinstance(event, ev.FilePlanned):
            if event.path:
                destination = Path(event.path)
                note = "new" if event.op == "CREATE" else "exists" if event.op == "SKIP" else ""
                suffix = f" ({note})" if note else ""
                self.console.print(f"{event.op.lower():<9} {destination}{suffix}")
            return
        if isinstance(event, ev.StageStarted) and event.stage_id == "apply_scaffold":
            if not self._dry_run:
                self.console.print("Applying scaffold...")
            return
        if isinstance(event, ev.FileWritten):
            return
        if isinstance(event, ev.FileWriteFailed):
            message = _redact(event.message)
            self.console.print(f"FAIL WRITE {event.path}: {message}")
            return
        if isinstance(event, ev.Warning):
            self._warnings.append(_redact(event.message))
            return
        if isinstance(event, ev.StageFailed):
            self._failed = event
            label = _stage_label(event.stage_id, INIT_STAGES)
            self.console.print(f"{label} FAIL: {_redact(event.message)}")
            return
        if isinstance(event, ev.CommandCompleted):
            if event.ok:
                if not self._dry_run:
                    project = self._project or Path(".")
                    self.console.print("\nProject initialized")
                    self.console.print(RULE_LINE)
                    self.console.print(f"- Location: {project}")
                    self.console.print("- Editable files:")
                    for line in _editable_scaffold_lines(project):
                        self.console.print(f"  - {line}")
                    self.console.print("")
                    self.console.print("- Next: run `opactx validate`, then `opactx build`.")
                    for warning in self._warnings:
                        self.console.print(f"Warning: {warning}")
            else:
                if self._failed:
                    self.console.print(f"Error: {_redact(self._failed.message)}")
                else:
                    self.console.print("Error: initialization failed")


class ValidateRichRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._checks: dict[str, str] = {}
        self._warnings: list[str] = []
        self._errors: list[str] = []

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            _print_header(self.console, event)
            return
        if isinstance(event, ev.StageCompleted):
            self._checks[event.stage_id] = event.status
            return
        if isinstance(event, ev.StageFailed):
            self._checks[event.stage_id] = "failed"
            self._errors.append(_redact(event.message))
            return
        if isinstance(event, ev.Warning):
            self._warnings.append(_redact(event.message))
            return
        if isinstance(event, ev.SchemaValidationFailed):
            for item in event.errors[:20]:
                self._errors.append(f"{item['path']}: {_redact(item['message'])}")
            return
        if isinstance(event, ev.CommandCompleted):
            self._render_summary()

    def _render_summary(self) -> None:
        self.console.print("Validation")
        self.console.print(RULE_LINE)
        table = Table(show_header=True, box=box.MINIMAL)
        table.add_column("Check", style="bold")
        table.add_column("Status")
        for stage_id, _label in VALIDATE_STAGES:
            label = VALIDATE_CHECK_LABELS.get(stage_id, _label)
            status = self._checks.get(stage_id, "skipped")
            detail = ""
            if stage_id == "resolve_plugins" and status == "skipped":
                detail = "skipped (run --strict)"
            if stage_id == "schema_check" and status == "partial":
                detail = "partial (sources not fetched)"
            if status == "skipped" and stage_id != "resolve_plugins" and detail == "":
                detail = "skipped"
            if status == "success":
                detail = "ok"
            if status == "failed":
                detail = "fail"
            cell = _validate_status_text(status, detail)
            table.add_row(label, cell)
        self.console.print(table)
        if self._warnings:
            warnings_text = Text("\n".join(f"- {warning}" for warning in self._warnings), style="orange1")
            self.console.print(
                Panel(
                    warnings_text,
                    title="[orange1]Warnings[/orange1]",
                    box=box.ROUNDED,
                    title_align="left",
                    border_style="orange1",
                )
            )
        if self._errors:
            errors_text = "\n".join(f"- {error}" for error in self._errors)
            self.console.print(Panel(errors_text, title="Errors", box=box.ROUNDED, title_align="left"))
        self.console.print("")
        has_failed = any(status == "failed" for status in self._checks.values())
        has_partial = any(status == "partial" for status in self._checks.values())
        if has_failed:
            overall = "failed"
        elif has_partial or self._warnings:
            overall = "partial"
        else:
            overall = "success"
        self.console.print(Text.assemble(Text("Validation status: "), _status_badge(overall)))


class ValidatePlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._checks: dict[str, str] = {}
        self._warnings: list[str] = []
        self._errors: list[str] = []

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            _print_header(self.console, event)
            return
        if isinstance(event, ev.StageStarted):
            label = VALIDATE_CHECK_LABELS.get(event.stage_id, _stage_label(event.stage_id, VALIDATE_STAGES))
            index = _stage_index(event.stage_id, VALIDATE_STAGES)
            line = _format_stage_start_line(index, label, len(VALIDATE_STAGES))
            self.console.print(line)
            return
        if isinstance(event, ev.SchemaLoaded):
            if event.path:
                self.console.print(f"Schema loaded: {event.path}")
            return
        if isinstance(event, ev.StageCompleted):
            self._checks[event.stage_id] = event.status
            return
        if isinstance(event, ev.StageFailed):
            self._checks[event.stage_id] = "failed"
            self._errors.append(_redact(event.message))
            label = VALIDATE_CHECK_LABELS.get(event.stage_id, _stage_label(event.stage_id, VALIDATE_STAGES))
            self.console.print(f"{label} FAIL: {_redact(event.message)}")
            return
        if isinstance(event, ev.PluginResolved):
            self.console.print(f"Plugin OK: {event.kind} {event.type_key}")
            return
        if isinstance(event, ev.PluginMissing):
            self.console.print(f"Plugin missing: {event.kind} {event.type_key}")
            return
        if isinstance(event, ev.Warning):
            self._warnings.append(_redact(event.message))
            return
        if isinstance(event, ev.SchemaValidationFailed):
            for item in event.errors[:20]:
                self._errors.append(f"{item['path']}: {_redact(item['message'])}")
            return
        if isinstance(event, ev.CommandCompleted):
            self.console.print("Preflight checks (no source fetching)")
            self.console.print(RULE_LINE)
            for stage_id, _label in VALIDATE_STAGES:
                label = VALIDATE_CHECK_LABELS.get(stage_id, _label)
                status = self._checks.get(stage_id, "skipped")
                if stage_id == "resolve_plugins" and status == "skipped":
                    self.console.print(f"{label}: skipped (run --strict)")
                elif stage_id == "schema_check" and status == "partial":
                    self.console.print(f"{label}: partial (sources not fetched)")
                else:
                    label_status = status
                    if status == "success":
                        label_status = "OK"
                    elif status == "failed":
                        label_status = "FAIL"
                    self.console.print(f"{label}: {label_status}")
            if self._warnings:
                self.console.print("")
                self.console.print("Warnings")
                self.console.print(RULE_LINE)
                for warning in self._warnings:
                    self.console.print(f"- {warning}")
            if self._errors:
                self.console.print("")
                self.console.print("Errors")
                self.console.print(RULE_LINE)
                for error in self._errors:
                    self.console.print(f"- {error}")
            self.console.print("")
            if event.ok:
                self.console.print("VALIDATE OK")
            else:
                self.console.print("VALIDATE FAIL")


class ValidateJsonRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._checks: dict[str, str] = {}
        self._warnings: list[str] = []
        self._errors: list[dict[str, str]] = []
        self._ok = True

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.StageCompleted):
            self._checks[event.stage_id] = event.status
            return
        if isinstance(event, ev.StageFailed):
            self._checks[event.stage_id] = "failed"
            self._errors.append({"check": event.stage_id, "message": _redact(event.message)})
            return
        if isinstance(event, ev.Warning):
            self._warnings.append(_redact(event.message))
            return
        if isinstance(event, ev.SchemaValidationFailed):
            for item in event.errors[:20]:
                self._errors.append(
                    {
                        "check": "schema_check",
                        "message": _redact(item["message"]),
                        "path": item["path"],
                    }
                )
            return
        if isinstance(event, ev.CommandCompleted):
            self._ok = event.ok
            payload = {
                "ok": self._ok,
                "checks": self._checks,
                "warnings": self._warnings,
                "errors": self._errors,
            }
            self.console.print(json.dumps(payload, indent=2, sort_keys=True))


class InspectRichRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._bundle_path: Path | None = None
        self._manifest: ev.ManifestRead | None = None
        self._summary: ev.ContextSummary | None = None
        self._path: ev.PathExtracted | None = None
        self._failed: ev.StageFailed | None = None
        self._data_bytes: int | None = None

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            self._bundle_path = Path(event.project_dir) if event.project_dir else None
            _print_header(self.console, event)
            return
        if isinstance(event, ev.StageStarted):
            label = _stage_label(event.stage_id, INSPECT_STAGES)
            index = _stage_index(event.stage_id, INSPECT_STAGES)
            line = _format_stage_start_line(index, label, len(INSPECT_STAGES))
            self.console.print(line)
            return
        if isinstance(event, ev.BundleOpened):
            if event.path:
                self.console.print(f"Inspect bundle: {event.path}")
            return
        if isinstance(event, ev.ManifestRead):
            self._manifest = event
            return
        if isinstance(event, ev.ContextSummary):
            self._summary = event
            return
        if isinstance(event, ev.DataRead):
            self._data_bytes = event.bytes
            return
        if isinstance(event, ev.PathExtracted):
            self._path = event
            return
        if isinstance(event, ev.StageFailed):
            self._failed = event
            return
        if isinstance(event, ev.CommandCompleted):
            if event.ok:
                self._render()
            else:
                self._render_error()

    def _render(self) -> None:
        if self._manifest:
            bundle_path = self._bundle_path or Path(".")
            size_line = ""
            if self._data_bytes is not None:
                size_line = f"Size:      {_format_bytes(self._data_bytes)}"
            body_lines = [
                f"Path:      {bundle_path}",
                f"Revision:  {self._manifest.revision}",
                f"Roots:     {self._manifest.roots}",
            ]
            if size_line:
                body_lines.append(size_line)
            body = "\n".join(body_lines)
            self.console.print(Panel(body, title="Bundle summary", box=box.ROUNDED, title_align="left"))
        if self._summary:
            table = Table(show_header=True, box=box.MINIMAL)
            table.add_column("SECTION")
            table.add_column("STATUS")
            table.add_column("KEYS", justify="right")
            counts = self._summary.counts or {}
            for key in self._summary.keys:
                table.add_row(key, STATUS_GLYPHS["success"], str(counts.get(key, 0)))
            self.console.print(
                Panel(table, title="Context overview (data.context)", box=box.ROUNDED, title_align="left")
            )
        if self._path:
            body = "\n".join(
                [
                    f"TYPE: {self._path.value_type}",
                    "",
                    self._path.preview,
                ]
            )
            self.console.print(Panel(body, title=self._path.path_pointer, box=box.ROUNDED, title_align="left"))

    def _render_error(self) -> None:
        if not self._failed:
            self.console.print("[red]Inspect failed.[/red]")
            return
        body = "\n".join(
            [
                f"stage: {self._failed.stage_id}",
                f"error: {_redact(self._failed.message)}",
            ]
        )
        if self._failed.hint:
            body = "\n".join([body, f"hint: {_redact(self._failed.hint)}"])
        self.console.print(Panel(body, title="Inspect failed", box=box.ROUNDED, title_align="left"))


class InspectPlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._bundle_path: Path | None = None
        self._manifest: ev.ManifestRead | None = None
        self._summary: ev.ContextSummary | None = None
        self._path: ev.PathExtracted | None = None
        self._failed: ev.StageFailed | None = None
        self._data_bytes: int | None = None

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            self._bundle_path = Path(event.project_dir) if event.project_dir else None
            _print_header(self.console, event)
            return
        if isinstance(event, ev.ManifestRead):
            self._manifest = event
            return
        if isinstance(event, ev.ContextSummary):
            self._summary = event
            return
        if isinstance(event, ev.DataRead):
            self._data_bytes = event.bytes
            return
        if isinstance(event, ev.PathExtracted):
            self._path = event
            return
        if isinstance(event, ev.StageFailed):
            self._failed = event
            return
        if isinstance(event, ev.CommandCompleted):
            if event.ok:
                if self._manifest:
                    bundle_path = self._bundle_path or Path(".")
                    self.console.print("Bundle summary")
                    self.console.print(RULE_LINE)
                    self.console.print(f"Path: {bundle_path}")
                    self.console.print(f"Revision: {self._manifest.revision}")
                    self.console.print(f"Roots: {self._manifest.roots}")
                    if self._data_bytes is not None:
                        self.console.print(f"Size: {_format_bytes(self._data_bytes)}")
                if self._summary:
                    counts = self._summary.counts or {}
                    self.console.print("Context overview (data.context)")
                    self.console.print(RULE_LINE)
                    self.console.print(
                        "Context: "
                        + " ".join(
                            f"{key}={counts.get(key, 0)}" for key in self._summary.keys
                        )
                    )
                if self._path:
                    self.console.print(self._path.path_pointer)
                    self.console.print(RULE_LINE)
                    self.console.print(f"TYPE: {self._path.value_type}")
                    self.console.print("")
                    self.console.print(self._path.preview)
            else:
                if self._failed:
                    self.console.print(f"Error: {_redact(self._failed.message)}")
                else:
                    self.console.print("Error: inspect failed")


class InspectJsonRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._bundle_path: Path | None = None
        self._manifest: ev.ManifestRead | None = None
        self._summary: ev.ContextSummary | None = None
        self._path: ev.PathExtracted | None = None
        self._ok = True
        self._data_bytes: int | None = None

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            self._bundle_path = Path(event.project_dir) if event.project_dir else None
        if isinstance(event, ev.ManifestRead):
            self._manifest = event
        if isinstance(event, ev.ContextSummary):
            self._summary = event
        if isinstance(event, ev.DataRead):
            self._data_bytes = event.bytes
        if isinstance(event, ev.PathExtracted):
            self._path = event
        if isinstance(event, ev.CommandCompleted):
            self._ok = event.ok
            payload = {
                "ok": self._ok,
                "bundle_path": str(self._bundle_path) if self._bundle_path else None,
                "manifest": self._manifest.to_dict() if self._manifest else None,
                "summary": self._summary.to_dict() if self._summary else None,
                "path": self._path.to_dict() if self._path else None,
                "data_bytes": self._data_bytes,
            }
            self.console.print(json.dumps(payload, indent=2, sort_keys=True))


class ListPluginsRichRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            _print_header(self.console, event)
            return
        if isinstance(event, ev.PluginsDiscovered):
            table = Table(title=f"{event.kind} plugins", box=box.ROUNDED, title_justify="left")
            table.add_column("TYPE", style="bold")
            table.add_column("IMPL")
            for plugin in event.plugins:
                table.add_row(plugin["type_key"], plugin["impl"])
            self.console.print(table)


class ListPluginsPlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            _print_header(self.console, event)
            return
        if isinstance(event, ev.PluginsDiscovered):
            self.console.print(f"{event.kind} plugins:")
            for plugin in event.plugins:
                self.console.print(f"- {plugin['type_key']}: {plugin['impl']}")


class ListPluginsJsonRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._plugins: dict[str, list[dict[str, str]]] = {}

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.PluginsDiscovered):
            self._plugins[event.kind] = event.plugins
        if isinstance(event, ev.CommandCompleted):
            self.console.print(json.dumps({"ok": event.ok, "plugins": self._plugins}, indent=2, sort_keys=True))


class RunOpaRichRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._failed: ev.StageFailed | None = None

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            _print_header(self.console, event)
            return
        if isinstance(event, ev.OpaStartPlanned):
            bundle = event.bundle_path or Path("dist/bundle")
            policy = event.policy_paths[0] if event.policy_paths else Path("policy")
            body = "\n".join(
                [
                    f"Bundle:  {bundle}",
                    f"Policy:  {policy}",
                    f"Address: {event.address}",
                ]
            )
            self.console.print(
                Panel(body, title="Starting OPA (development mode)", box=box.ROUNDED, title_align="left")
            )
            return
        if isinstance(event, ev.OpaProcessStarted):
            self.console.print("\nOPA running (Ctrl+C to stop)")
            return
        if isinstance(event, ev.OpaStdout):
            if event.line:
                self.console.print(event.line)
            return
        if isinstance(event, ev.OpaStderr):
            if event.line:
                self.console.print(event.line)
            return
        if isinstance(event, ev.StageFailed):
            self._failed = event
            return
        if isinstance(event, ev.OpaProcessExited):
            body = f"Exit code: {event.exit_code}"
            self.console.print(Panel(body, title="OPA stopped", box=box.ROUNDED, title_align="left"))
            return
        if isinstance(event, ev.CommandCompleted) and not event.ok:
            self._print_error()

    def _print_error(self) -> None:
        if not self._failed:
            self.console.print("[red]OPA failed to start.[/red]")
            return
        body = "\n".join(
            [
                f"stage: {self._failed.stage_id}",
                f"error: {_redact(self._failed.message)}",
            ]
        )
        self.console.print(Panel(body, title="OPA failed", box=box.ROUNDED, title_align="left"))


class RunOpaPlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._failed: ev.StageFailed | None = None

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            _print_header(self.console, event)
            return
        if isinstance(event, ev.OpaStartPlanned):
            bundle = event.bundle_path or Path("dist/bundle")
            policy = event.policy_paths[0] if event.policy_paths else Path("policy")
            self.console.print(
                f"OPA START address={event.address} bundle={bundle} policy={policy}"
            )
            return
        if isinstance(event, ev.OpaProcessStarted):
            self.console.print(f"\nOPA PID {event.pid}")
            return
        if isinstance(event, ev.OpaStdout):
            if event.line:
                self.console.print(event.line)
            return
        if isinstance(event, ev.OpaStderr):
            if event.line:
                self.console.print(event.line)
            return
        if isinstance(event, ev.StageFailed):
            self._failed = event
            return
        if isinstance(event, ev.OpaProcessExited):
            self.console.print(f"\nOPA EXIT code={event.exit_code}")
            return
        if isinstance(event, ev.CommandCompleted) and not event.ok:
            if self._failed:
                self.console.print(f"Error: {_redact(self._failed.message)}")
            else:
                self.console.print("Error: OPA failed")


def _format_duration(elapsed_ms: float) -> str:
    if elapsed_ms < 1000:
        return f"{elapsed_ms:.0f}ms"
    seconds = elapsed_ms / 1000
    if seconds < 10:
        return f"{seconds:.2f}s"
    return f"{seconds:.1f}s"


def _format_bytes(num: int) -> str:
    size = float(num)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024 or unit == "GB":
            if unit == "B":
                return f"{size:.0f} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def _print_header(console: Console, event: ev.CommandStarted) -> None:
    project = event.project_dir or Path(".")
    config = event.config_path or Path("opactx.yaml")
    console.print(f"opactx v{__version__} | project: {project} | config: {config}\n{RULE_LINE}")


_REDACT_PATTERN = re.compile(
    r"(?i)\b(authorization|token|secret|password|api_key)\b\s*[:=]\s*[^\s]+"
)


def _redact(text: str) -> str:
    if not text:
        return text
    return _REDACT_PATTERN.sub(r"\1: <redacted>", text)


def _format_stage_line(
    index: int,
    label: str,
    status: str,
    elapsed_ms: float | None,
    progress: tuple[int, int] | None = None,
    note: str | None = None,
    total: int = 6,
    include_status_word: bool = False,
) -> str:
    glyph = STATUS_GLYPHS.get(status, "?")
    suffix = ""
    if status == "running":
        if progress:
            suffix = f" running ({progress[0]}/{progress[1]})"
        else:
            suffix = " running"
    elif status in {"skipped", "partial", "failed"} and not include_status_word:
        suffix = f" {status}"
    if include_status_word and status in {"success", "failed", "skipped", "partial"}:
        suffix = f" {_status_word(status)}"
    if note:
        suffix = f"{suffix} {note}".rstrip()
    duration = f"  {_format_duration(elapsed_ms)}" if elapsed_ms is not None else ""
    padding = "." * max(2, 28 - len(label))
    total_display = total if total > 0 else 0
    return f"[{index}/{total_display}] {label} {padding} {glyph}{suffix}{duration}"


def _format_stage_start_line(index: int, label: str, total: int, note: str | None = None) -> str:
    padding = "." * max(2, 28 - len(label))
    total_display = total if total > 0 else 0
    suffix = f" START"
    if note:
        suffix = f"{suffix} {note}"
    return f"[{index}/{total_display}] {label} {padding}{suffix}"


def _stage_label(stage_id: str, mapping: list[tuple[str, str]]) -> str:
    for key, label in mapping:
        if key == stage_id:
            return label
    return stage_id


def _stage_index(stage_id: str, mapping: list[tuple[str, str]]) -> int:
    for index, (key, _label) in enumerate(mapping, start=1):
        if key == stage_id:
            return index
    return 0


def _status_word(status: str) -> str:
    return {
        "success": "OK",
        "failed": "FAIL",
        "skipped": "SKIP",
        "partial": "PARTIAL",
    }.get(status, status.upper())


def _status_badge(status: str) -> Text:
    normalized = status.strip().lower()
    label = {
        "success": "ok",
        "failed": "fail",
        "skipped": "skip",
        "partial": "partial",
    }.get(normalized, normalized)
    style = {
        "success": "bold black on green3",
        "failed": "bold white on red3",
        "skipped": "bold white on grey35",
        "partial": "bold black on dark_orange3",
    }.get(normalized, "bold white on grey35")
    return Text(f" {label} ", style=style)


def _validate_status_text(status: str, detail: str) -> Text:
    normalized = status.strip().lower()
    style = {
        "success": "green",
        "failed": "red",
        "skipped": "bright_black",
        "partial": "orange1",
    }.get(normalized, "default")
    if detail:
        return Text(detail, style=style)
    return Text(_status_word(status).lower(), style=style)


def _scaffold_action_text(action: str) -> Text:
    normalized = action.strip().lower()
    style_map = {
        "create": "green",
        "overwrite": "yellow",
        "skip": "dim",
    }
    return Text(normalized, style=style_map.get(normalized, "bold"))


def _editable_scaffold_lines(project: Path) -> list[str]:
    schema_path = "schema/context.schema.yaml"
    if (project / "schema" / "context.schema.json").exists():
        schema_path = "schema/context.schema.json"
    lines = [
        "opactx.yaml: pipeline config (sources, transforms, schema, output).",
        f"{schema_path}: context contract.",
        "context/standards.yaml: required policy standards input.",
    ]
    if (project / "context" / "exceptions.yaml").exists():
        lines.append("context/exceptions.yaml: optional exception entries.")
    if (project / "policy").exists():
        lines.append("policy/*.rego: policy rules/modules.")
    return lines


def _sources_table(states: Iterable[SourceState], title: str | None = "Sources") -> Table:
    table = Table(title=title, show_header=True, box=box.MINIMAL, title_justify="left")
    table.add_column("NAME", style="bold")
    table.add_column("TYPE")
    table.add_column("STATUS")
    table.add_column("TIME", justify="right")
    table.add_column("NOTES")
    for state in states:
        glyph = {
            "waiting": STATUS_GLYPHS["pending"],
            "fetching": STATUS_GLYPHS["running"],
            "done": STATUS_GLYPHS["success"],
            "failed": STATUS_GLYPHS["failed"],
        }.get(state.status, "?")
        duration = _format_duration(state.elapsed_ms) if state.elapsed_ms is not None else ""
        table.add_row(
            state.name,
            state.source_type,
            f"{glyph} {state.status}",
            duration,
            _redact(state.note) if state.note else "",
        )
    return table


def _source_failure_panel(event: ev.SourceFetchFailed) -> Panel:
    body = "\n".join(
        [
            f"source: {event.name} (type={event.type_key})",
            f"error: {_redact(event.message)}",
            "hint: check credentials; run with --debug for details",
        ]
    )
    return Panel(body, title="Source fetch failed", box=box.ROUNDED, title_align="left")


def _schema_failure_panel(event: ev.SchemaValidationFailed) -> Panel:
    lines = [f"{item['path']}: {_redact(item['message'])}" for item in event.errors[:20]]
    if len(event.errors) > 20:
        lines.append(f"...and {len(event.errors) - 20} more")
    body = "\n".join(
        [
            *lines,
            "",
            "Hint:",
            "- Check context/standards.yaml and connector output shape",
            "- Run: opactx inspect dist/bundle --path /sources/iam",
        ]
    )
    return Panel(
        body,
        title=f"Schema validation failed ({len(event.errors)} errors)",
        box=box.ROUNDED,
        title_align="left",
    )


def _stage_failure_panel(event: ev.StageFailed) -> Panel:
    body = "\n".join(
        [
            f"stage: {event.stage_id}",
            f"error: {_redact(event.message)}",
        ]
    )
    if event.hint:
        body = "\n".join([body, f"hint: {_redact(event.hint)}"])
    return Panel(body, title="Build failed", box=box.ROUNDED, title_align="left")

