from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable

from opactx.core import events as ev


def inspect_events(
    *,
    bundle_path: Path,
    pointer: str | None = None,
) -> Iterable[ev.OpactxEvent]:
    bundle_path = bundle_path.resolve()
    yield ev.CommandStarted(
        command="inspect",
        project_dir=bundle_path,
        options={"pointer": pointer},
    )

    yield ev.StageStarted(command="inspect", stage_id="open_bundle", label="Open bundle")
    started = time.perf_counter()
    if not bundle_path.exists():
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="inspect",
            stage_id="open_bundle",
            duration_ms=duration_ms,
            error_code="bundle_missing",
            message=f"Bundle not found: {bundle_path}",
        )
        yield ev.CommandCompleted(command="inspect", ok=False, exit_code=2)
        return
    if bundle_path.is_file():
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="inspect",
            stage_id="open_bundle",
            duration_ms=duration_ms,
            error_code="bundle_type",
            message="Bundle path must be a directory in v1.",
        )
        yield ev.CommandCompleted(command="inspect", ok=False, exit_code=2)
        return

    yield ev.BundleOpened(command="inspect", path=bundle_path)
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="inspect",
        stage_id="open_bundle",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.StageStarted(command="inspect", stage_id="read_manifest", label="Read manifest")
    started = time.perf_counter()
    manifest_path = bundle_path / ".manifest"
    manifest = _read_json(manifest_path)
    if isinstance(manifest, str):
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="inspect",
            stage_id="read_manifest",
            duration_ms=duration_ms,
            error_code="manifest_error",
            message=manifest,
        )
        yield ev.CommandCompleted(command="inspect", ok=False, exit_code=2)
        return
    yield ev.ManifestRead(
        command="inspect",
        path=manifest_path,
        revision=str(manifest.get("revision")) if isinstance(manifest, dict) else None,
        roots=manifest.get("roots") if isinstance(manifest, dict) else None,
    )
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="inspect",
        stage_id="read_manifest",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.StageStarted(command="inspect", stage_id="read_data", label="Read data")
    started = time.perf_counter()
    data_path = bundle_path / "data.json"
    data = _read_json(data_path)
    if isinstance(data, str):
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="inspect",
            stage_id="read_data",
            duration_ms=duration_ms,
            error_code="data_error",
            message=data,
        )
        yield ev.CommandCompleted(command="inspect", ok=False, exit_code=2)
        return
    raw_bytes = data_path.read_bytes()
    yield ev.DataRead(command="inspect", path=data_path, bytes=len(raw_bytes))
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="inspect",
        stage_id="read_data",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.StageStarted(command="inspect", stage_id="summarize_context", label="Summarize context")
    started = time.perf_counter()
    context = data.get("context") if isinstance(data, dict) else {}
    counts = {}
    keys = []
    if isinstance(context, dict):
        for key in ["standards", "exceptions", "sources"]:
            keys.append(key)
            value = context.get(key, {})
            counts[key] = len(value) if isinstance(value, dict) else 0
    yield ev.ContextSummary(command="inspect", keys=keys, counts=counts)
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="inspect",
        stage_id="summarize_context",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.StageStarted(command="inspect", stage_id="extract_path", label="Extract path")
    started = time.perf_counter()
    if not pointer:
        duration_ms = _elapsed_ms(started)
        yield ev.StageCompleted(
            command="inspect",
            stage_id="extract_path",
            duration_ms=duration_ms,
            status="skipped",
        )
        yield ev.CommandCompleted(command="inspect", ok=True, exit_code=0)
        return

    result = _extract_pointer(data, pointer)
    if isinstance(result, str):
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="inspect",
            stage_id="extract_path",
            duration_ms=duration_ms,
            error_code="path_error",
            message=result,
        )
        yield ev.CommandCompleted(command="inspect", ok=False, exit_code=2)
        return
    preview = _preview_value(result)
    yield ev.PathExtracted(
        command="inspect",
        path_pointer=pointer,
        value_type=type(result).__name__,
        preview=preview,
    )
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="inspect",
        stage_id="extract_path",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.CommandCompleted(command="inspect", ok=True, exit_code=0)


def _read_json(path: Path) -> dict[str, Any] | str:
    if not path.exists():
        return f"Missing file: {path}"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return f"Invalid JSON: {path}"


def _extract_pointer(data: Any, pointer: str) -> Any | str:
    if pointer in {"", "/"}:
        return data
    if not pointer.startswith("/"):
        return "Pointer must start with '/'."
    parts = pointer.lstrip("/").split("/")
    current = data
    for part in parts:
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            if not part.isdigit():
                return f"Expected list index at '{part}'."
            index = int(part)
            if index >= len(current):
                return f"Index out of range at '{part}'."
            current = current[index]
        elif isinstance(current, dict):
            if part not in current:
                return f"Key not found: {part}"
            current = current[part]
        else:
            return f"Cannot traverse into {type(current).__name__} at '{part}'."
    return current


def _preview_value(value: Any) -> str:
    try:
        rendered = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False)
    except TypeError:
        rendered = str(value)
    if len(rendered) > 400:
        return rendered[:400] + "..."
    return rendered


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000
