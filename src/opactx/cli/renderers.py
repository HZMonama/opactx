from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from opactx.core import events as ev


STAGES_BUILD: list[tuple[str, str]] = [
    ("load_config", "Load config"),
    ("load_intent", "Load intent context"),
    ("fetch_sources", "Fetch sources"),
    ("normalize", "Normalize"),
    ("validate_schema", "Validate schema"),
    ("write_bundle", "Write bundle"),
]

STAGES_VALIDATE: list[tuple[str, str]] = [
    ("load_config", "Config"),
    ("load_schema", "Schema"),
    ("load_intent", "Intent"),
    ("resolve_plugins", "Plugins"),
    ("schema_check", "Schema check"),
]


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
        self.stage_status = {name: "pending" for name, _ in STAGES_BUILD}
        self.stage_elapsed: dict[str, float] = {}
        self.sources: dict[str, SourceState] = {}
        self._live: Live | None = None
        self._schema_path: Path | None = None
        self._bundle_info: dict[str, str] | None = None
        self._dry_run = False
        self._output_dir: Path | None = None
        self._source_failure: ev.SourceFetchFailed | None = None
        self._schema_failure: ev.SchemaValidationFailed | None = None
        self._stage_failure: ev.StageFailed | None = None

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
        if isinstance(event, ev.SchemaLoaded):
            self._schema_path = Path(event.path) if event.path else None
            return
        if isinstance(event, ev.SchemaValidationFailed):
            self._schema_failure = event
            return
        if isinstance(event, ev.SourceFetchStarted):
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
            elif self._stage_failure:
                self.console.print(_stage_failure_panel(self._stage_failure))
            return
        if self._dry_run:
            self.console.print("Dry run complete (no files written)")
            if self._output_dir:
                self.console.print(f"Would write: {self._output_dir / 'data.json'}")
                self.console.print(f"Would write: {self._output_dir / '.manifest'}")
            return
        if self._bundle_info:
            body = "\n".join(
                [
                    f"Path:     {self._bundle_info['out_dir']}",
                    f"Files:    {self._bundle_info['files']}",
                    f"Revision: {self._bundle_info['revision']}",
                    f"Schema:   {self._schema_path}",
                    f"Sources:  {len(self.sources)}",
                    "",
                    "Next:",
                    f"- opactx inspect {self._bundle_info['out_dir']}",
                ]
            )
            self.console.print(Panel(body, title="Build complete", box=box.ROUNDED, title_align="left"))

    def _print_header(self, event: ev.CommandStarted) -> None:
        project = event.project_dir or Path(".")
        config = event.config_path or Path("opactx.yaml")
        self.console.print(f"opactx | project: {project} | config: {config}")
        self.console.print("-" * 64)
        self.console.print("Building policy context bundle (OPA-compatible)")

    def _refresh(self) -> None:
        if self._live:
            self._live.update(self._render())

    def _render(self) -> Group:
        lines = []
        for index, (stage_id, label) in enumerate(STAGES_BUILD, start=1):
            status = self.stage_status.get(stage_id, "pending")
            elapsed = self.stage_elapsed.get(stage_id)
            lines.append(_format_stage_line(index, label, status, elapsed))
        renderables: list = [Text("\n".join(lines))]
        if self.sources:
            renderables.append(_sources_table(self.sources.values()))
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

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            self._dry_run = bool(event.options and event.options.get("dry_run"))
            self.console.print(f"opactx | project: {event.project_dir} | config: {event.config_path}")
            self.console.print("-" * 64)
            self.console.print("Building policy context bundle (OPA-compatible)")
            return
        if isinstance(event, ev.StageCompleted):
            label = _stage_label(event.stage_id, STAGES_BUILD)
            status = "OK" if event.status == "success" else event.status.upper()
            duration = _format_duration(event.duration_ms)
            self.console.print(f"{label}: {status} {duration}")
            return
        if isinstance(event, ev.StageFailed):
            label = _stage_label(event.stage_id, STAGES_BUILD)
            duration = _format_duration(event.duration_ms)
            self.console.print(f"{label}: FAIL {duration}")
            self._stage_failure = event
            return
        if isinstance(event, ev.SchemaLoaded):
            self._schema_path = Path(event.path) if event.path else None
            return
        if isinstance(event, ev.SchemaValidationFailed):
            self._schema_failure = event
            return
        if isinstance(event, ev.SourceFetchCompleted):
            self.console.print(_format_source_line(event.name, "done", event.duration_ms, None))
            return
        if isinstance(event, ev.SourceFetchFailed):
            self._source_failure = event
            self.console.print(_format_source_line(event.name, "failed", event.duration_ms, None))
            return
        if isinstance(event, ev.BundleWriteStarted):
            self._output_dir = Path(event.out_dir) if event.out_dir else None
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
                self.console.print(f"error: {self._source_failure.message}")
                return
            if self._schema_failure:
                self.console.print("Schema validation failed")
                for item in self._schema_failure.errors[:20]:
                    self.console.print(f"{item['path']}: {item['message']}")
                return
            if self._stage_failure:
                self.console.print(f"Error: {self._stage_failure.message}")
                return
            return
        if self._dry_run:
            self.console.print("Dry run complete (no files written)")
            if self._output_dir:
                self.console.print(f"Would write: {self._output_dir / 'data.json'}")
                self.console.print(f"Would write: {self._output_dir / '.manifest'}")
            return
        if self._bundle_info:
            self.console.print("Build complete")
            self.console.print(f"Path: {self._bundle_info['out_dir']}")
            self.console.print(f"Files: {self._bundle_info['files']}")
            self.console.print(f"Revision: {self._bundle_info['revision']}")
            self.console.print(f"Schema: {self._schema_path}")
            self.console.print(f"Sources: {self._bundle_info and ''}")


class InitRichRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._actions: list[tuple[str, Path]] = []
        self._options: dict[str, bool] = {}

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.CommandStarted):
            self._options = event.options or {}
            return
        if isinstance(event, ev.FilePlanned):
            if event.path:
                self._actions.append((event.op, Path(event.path)))
            return
        if isinstance(event, ev.StageCompleted) and event.stage_id == "plan":
            self._print_actions()
            return
        if isinstance(event, ev.CommandCompleted) and event.ok:
            self._print_summary()

    def _print_actions(self) -> None:
        title = _init_label(
            minimal=self._options.get("minimal", False),
            with_examples=self._options.get("with_examples", False),
            no_policy=self._options.get("no_policy", False),
        )
        self.console.print("")
        table = Table(title=title, box=box.ROUNDED, title_align="left")
        table.add_column("Action", style="bold")
        table.add_column("Path", overflow="fold")
        table.add_column("Note", style="dim")
        for action, destination in self._actions:
            style = {
                "CREATE": "green",
                "OVERWRITE": "yellow",
                "SKIP": "dim",
            }.get(action, "")
            note = "new" if action == "CREATE" else "exists" if action == "SKIP" else ""
            table.add_row(action, str(destination), note, style=style)
        self.console.print(table)

    def _print_summary(self) -> None:
        self.console.print("")
        self.console.print("[green]✓ Successfully initialized:[/green] .")
        self.console.print("")
        steps = "\n".join(
            [
                "- opactx build (outputs to dist/bundle/)",
                "- Edit opactx.yaml sources and context/standards.yaml",
            ]
        )
        self.console.print(Panel(steps, title="Next steps", box=box.ROUNDED, expand=False, title_align="left"))


class InitPlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._actions: list[tuple[str, Path]] = []

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.FilePlanned):
            if event.path:
                self._actions.append((event.op, Path(event.path)))
            return
        if isinstance(event, ev.StageCompleted) and event.stage_id == "plan":
            for op, path in self._actions:
                note = "new" if op == "CREATE" else "exists" if op == "SKIP" else ""
                suffix = f" ({note})" if note else ""
                self.console.print(f"{op} {path}{suffix}")
            return
        if isinstance(event, ev.CommandCompleted) and event.ok:
            self.console.print("Successfully initialized.")
            self.console.print("Next steps: opactx build")


class ValidateRichRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._checks: dict[str, str] = {}
        self._warnings: list[str] = []
        self._errors: list[str] = []

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.StageCompleted):
            self._checks[event.stage_id] = event.status
            return
        if isinstance(event, ev.StageFailed):
            self._checks[event.stage_id] = "failed"
            self._errors.append(event.message)
            return
        if isinstance(event, ev.Warning):
            self._warnings.append(event.message)
            return
        if isinstance(event, ev.SchemaValidationFailed):
            for item in event.errors[:20]:
                self._errors.append(f"{item['path']}: {item['message']}")
            return
        if isinstance(event, ev.CommandCompleted):
            self._render_summary()

    def _render_summary(self) -> None:
        for stage_id, label in STAGES_VALIDATE:
            status = self._checks.get(stage_id, "skipped")
            if status == "success":
                self.console.print(f"{label} OK")
            elif status == "partial":
                self.console.print(f"{label}: partial")
            elif status == "skipped":
                self.console.print(f"{label}: skipped")
            else:
                self.console.print(f"{label} FAILED")
        for warning in self._warnings:
            self.console.print(f"[yellow]Warning:[/yellow] {warning}")
        for error in self._errors:
            self.console.print(f"[red]Error:[/red] {error}")


class ValidatePlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._checks: dict[str, str] = {}
        self._warnings: list[str] = []
        self._errors: list[str] = []

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.StageCompleted):
            self._checks[event.stage_id] = event.status
            return
        if isinstance(event, ev.StageFailed):
            self._checks[event.stage_id] = "failed"
            self._errors.append(event.message)
            return
        if isinstance(event, ev.Warning):
            self._warnings.append(event.message)
            return
        if isinstance(event, ev.SchemaValidationFailed):
            for item in event.errors[:20]:
                self._errors.append(f"{item['path']}: {item['message']}")
            return
        if isinstance(event, ev.CommandCompleted):
            for stage_id, label in STAGES_VALIDATE:
                status = self._checks.get(stage_id, "skipped")
                self.console.print(f"{label}: {status}")
            for warning in self._warnings:
                self.console.print(f"Warning: {warning}")
            for error in self._errors:
                self.console.print(f"Error: {error}")


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
            self._errors.append({"check": event.stage_id, "message": event.message})
            return
        if isinstance(event, ev.Warning):
            self._warnings.append(event.message)
            return
        if isinstance(event, ev.SchemaValidationFailed):
            for item in event.errors[:20]:
                self._errors.append({"check": "schema_check", "message": item["message"], "path": item["path"]})
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
        self._manifest: ev.ManifestRead | None = None
        self._summary: ev.ContextSummary | None = None
        self._path: ev.PathExtracted | None = None

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.ManifestRead):
            self._manifest = event
            return
        if isinstance(event, ev.ContextSummary):
            self._summary = event
            return
        if isinstance(event, ev.PathExtracted):
            self._path = event
            return
        if isinstance(event, ev.CommandCompleted) and event.ok:
            self._render()

    def _render(self) -> None:
        if self._manifest:
            body = "\n".join(
                [
                    f"Revision: {self._manifest.revision}",
                    f"Roots:    {', '.join(self._manifest.roots or [])}",
                ]
            )
            self.console.print(Panel(body, title="Bundle", box=box.ROUNDED, title_align="left"))
        if self._summary:
            table = Table(title="Context summary", box=box.ROUNDED, title_align="left")
            table.add_column("Section")
            table.add_column("Count", justify="right")
            counts = self._summary.counts or {}
            for key in self._summary.keys:
                table.add_row(key, str(counts.get(key, 0)))
            self.console.print(table)
        if self._path:
            body = "\n".join(
                [
                    f"Path: {self._path.path_pointer}",
                    f"Type: {self._path.value_type}",
                    "",
                    self._path.preview,
                ]
            )
            self.console.print(Panel(body, title="Path result", box=box.ROUNDED, title_align="left"))


class InspectPlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._manifest: ev.ManifestRead | None = None
        self._summary: ev.ContextSummary | None = None
        self._path: ev.PathExtracted | None = None

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.ManifestRead):
            self._manifest = event
            return
        if isinstance(event, ev.ContextSummary):
            self._summary = event
            return
        if isinstance(event, ev.PathExtracted):
            self._path = event
            return
        if isinstance(event, ev.CommandCompleted) and event.ok:
            if self._manifest:
                self.console.print(f"Revision: {self._manifest.revision}")
                self.console.print(f"Roots: {', '.join(self._manifest.roots or [])}")
            if self._summary:
                counts = self._summary.counts or {}
                for key in self._summary.keys:
                    self.console.print(f"{key}: {counts.get(key, 0)}")
            if self._path:
                self.console.print(f"Path {self._path.path_pointer}: {self._path.preview}")


class InspectJsonRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console
        self._manifest: ev.ManifestRead | None = None
        self._summary: ev.ContextSummary | None = None
        self._path: ev.PathExtracted | None = None
        self._ok = True

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.ManifestRead):
            self._manifest = event
        if isinstance(event, ev.ContextSummary):
            self._summary = event
        if isinstance(event, ev.PathExtracted):
            self._path = event
        if isinstance(event, ev.CommandCompleted):
            self._ok = event.ok
            payload = {
                "ok": self._ok,
                "manifest": self._manifest.to_dict() if self._manifest else None,
                "summary": self._summary.to_dict() if self._summary else None,
                "path": self._path.to_dict() if self._path else None,
            }
            self.console.print(json.dumps(payload, indent=2, sort_keys=True))


class ListPluginsRichRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console

    def handle(self, event: ev.OpactxEvent) -> None:
        if isinstance(event, ev.PluginsDiscovered):
            table = Table(title=f"{event.kind} plugins", box=box.ROUNDED, title_align="left")
            table.add_column("TYPE", style="bold")
            table.add_column("IMPL")
            for plugin in event.plugins:
                table.add_row(plugin["type_key"], plugin["impl"])
            self.console.print(table)


class ListPluginsPlainRenderer(Renderer):
    def __init__(self, console: Console):
        self.console = console

    def handle(self, event: ev.OpactxEvent) -> None:
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


def _format_duration(elapsed_ms: float) -> str:
    if elapsed_ms < 1:
        return f"{elapsed_ms * 1000:.0f}ms"
    if elapsed_ms < 10:
        return f"{elapsed_ms:.2f}s"
    return f"{elapsed_ms:.1f}s"


def _format_stage_line(index: int, label: str, status: str, elapsed_ms: float | None) -> str:
    glyph = {
        "pending": "⏸",
        "running": "⠋",
        "success": "✅",
        "failed": "❌",
        "skipped": "⏭",
        "partial": "⚠️",
    }.get(status, "?")
    suffix = ""
    if status == "running":
        suffix = " running"
    duration = f"  {_format_duration(elapsed_ms)}" if elapsed_ms is not None else ""
    padding = "." * max(2, 28 - len(label))
    return f"[{index}/6] {label} {padding} {glyph}{suffix}{duration}"


def _stage_label(stage_id: str, mapping: list[tuple[str, str]]) -> str:
    for key, label in mapping:
        if key == stage_id:
            return label
    return stage_id


def _sources_table(states: Iterable[SourceState]) -> Table:
    table = Table(title="Sources", show_header=True, box=box.ROUNDED, title_align="left")
    table.add_column("NAME", style="bold")
    table.add_column("TYPE")
    table.add_column("STATUS")
    table.add_column("TIME", justify="right")
    table.add_column("NOTES")
    for state in states:
        glyph = {
            "waiting": "⏸",
            "fetching": "⠋",
            "done": "✅",
            "failed": "❌",
        }.get(state.status, "?")
        duration = _format_duration(state.elapsed_ms) if state.elapsed_ms is not None else ""
        table.add_row(
            state.name,
            state.source_type,
            f"{glyph} {state.status}",
            duration,
            state.note or "",
        )
    return table


def _format_source_line(name: str, status: str, elapsed_ms: float | None, note: str | None) -> str:
    duration = _format_duration(elapsed_ms) if elapsed_ms is not None else ""
    suffix = f" ({note})" if note else ""
    return f"source {name}: {status} {duration}{suffix}"


def _source_failure_panel(event: ev.SourceFetchFailed) -> Panel:
    body = "\n".join(
        [
            f"source: {event.name} (type={event.type_key})",
            f"error: {event.message}",
            "hint: check credentials; run with --debug for details",
        ]
    )
    return Panel(body, title="Source fetch failed", box=box.ROUNDED, title_align="left")


def _schema_failure_panel(event: ev.SchemaValidationFailed) -> Panel:
    lines = [f"{item['path']}: {item['message']}" for item in event.errors[:20]]
    if len(event.errors) > 20:
        lines.append(f"...and {len(event.errors) - 20} more")
    body = "\n".join(
        [
            f"{event.schema_path}",
            "",
            *lines,
            "",
            "Hints:",
            "- Check context/standards.yaml and connector output shape.",
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
            f"error: {event.message}",
        ]
    )
    if event.hint:
        body = "\n".join([body, f"hint: {event.hint}"])
    return Panel(body, title="Build failed", box=box.ROUNDED, title_align="left")


def _init_label(minimal: bool, with_examples: bool, no_policy: bool) -> str:
    if minimal:
        return "Initializing minimal template"
    if with_examples:
        return "Initializing template with examples"
    if no_policy:
        return "Initializing template without policy"
    return "Initializing standard template"
