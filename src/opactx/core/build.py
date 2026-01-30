from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
import time
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from jsonschema import Draft202012Validator

from opactx.config.load import ConfigError, load_config, load_yaml_mapping
from opactx.config.model import Config, Transform
from opactx.core import events as ev
from opactx.plugins.registry import load_source, load_transform
from opactx.transforms.builtin import canonicalize


def build_events(
    *,
    project_dir: Path,
    config_path: Path | None = None,
    output_dir: Path | None = None,
    clean: bool = False,
    dry_run: bool = False,
    fail_fast: bool = True,
    debug: bool = False,
) -> Iterable[ev.OpactxEvent]:
    project_dir = project_dir.resolve()
    config_path = config_path or Path("opactx.yaml")
    if not config_path.is_absolute():
        config_path = project_dir / config_path

    options = {
        "output_dir": str(output_dir) if output_dir else None,
        "clean": clean,
        "dry_run": dry_run,
        "fail_fast": fail_fast,
    }
    yield ev.CommandStarted(
        command="build",
        project_dir=project_dir,
        config_path=config_path,
        options=options,
    )

    yield ev.StageStarted(command="build", stage_id="load_config", label="Load config")
    started = time.perf_counter()
    try:
        config = load_config(project_dir, config_path)
    except (ConfigError, ValueError) as exc:
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="build",
            stage_id="load_config",
            duration_ms=duration_ms,
            error_code="config_error",
            message=str(exc),
        )
        yield ev.CommandCompleted(command="build", ok=False, exit_code=2)
        return
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="build",
        stage_id="load_config",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.StageStarted(command="build", stage_id="load_intent", label="Load intent context")
    started = time.perf_counter()
    context_dir = Path(config.context_dir)
    if not context_dir.is_absolute():
        context_dir = project_dir / context_dir
    try:
        standards = load_yaml_mapping(context_dir / "standards.yaml", required=True)
        exceptions = load_yaml_mapping(context_dir / "exceptions.yaml", required=False)
    except ConfigError as exc:
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="build",
            stage_id="load_intent",
            duration_ms=duration_ms,
            error_code="intent_error",
            message=str(exc),
        )
        yield ev.CommandCompleted(command="build", ok=False, exit_code=2)
        return
    intent = {"standards": standards, "exceptions": exceptions}
    if not _is_json_serializable(intent):
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="build",
            stage_id="load_intent",
            duration_ms=duration_ms,
            error_code="intent_error",
            message="Context is not JSON-serializable.",
        )
        yield ev.CommandCompleted(command="build", ok=False, exit_code=2)
        return
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="build",
        stage_id="load_intent",
        duration_ms=duration_ms,
        status="success",
    )

    sources_result = _run_stage_fetch_sources(project_dir, config, fail_fast=fail_fast)
    for item in sources_result.events:
        yield item
    if sources_result.failed:
        yield ev.CommandCompleted(command="build", ok=False, exit_code=2)
        return
    sources = sources_result.value or {}

    yield ev.StageStarted(command="build", stage_id="normalize", label="Normalize")
    started = time.perf_counter()
    normalize_result = _normalize(project_dir, config, intent, sources)
    if isinstance(normalize_result, str):
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="build",
            stage_id="normalize",
            duration_ms=duration_ms,
            error_code="transform_error",
            message=normalize_result,
        )
        yield ev.CommandCompleted(command="build", ok=False, exit_code=2)
        return
    canonical = normalize_result
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="build",
        stage_id="normalize",
        duration_ms=duration_ms,
        status="success",
    )

    schema_result = _run_stage_validate_schema(project_dir, config, canonical)
    for item in schema_result.events:
        yield item
    if schema_result.failed:
        yield ev.CommandCompleted(command="build", ok=False, exit_code=2)
        return
    schema_path = schema_result.schema_path

    output_path = _resolve_output_dir(project_dir, config, output_dir)
    data_bytes = _stable_json_bytes({"context": canonical})
    revision = hashlib.sha256(data_bytes).hexdigest()

    if dry_run:
        yield ev.BundleWriteStarted(command="build", out_dir=output_path)
        yield ev.StageCompleted(
            command="build",
            stage_id="write_bundle",
            duration_ms=0.0,
            status="skipped",
        )
        yield ev.CommandCompleted(command="build", ok=True, exit_code=0)
        return

    write_result = _run_stage_write_bundle(
        output_path,
        data_bytes,
        revision,
        clean=clean,
        tarball=config.output.tarball,
    )
    for item in write_result.events:
        yield item
    if write_result.failed:
        yield ev.CommandCompleted(command="build", ok=False, exit_code=2)
        return

    yield ev.BundleWritten(
        command="build",
        out_dir=output_path,
        revision=revision,
        files=["data.json", ".manifest"],
    )
    yield ev.CommandCompleted(command="build", ok=True, exit_code=0)


class _StageResultWithEvents:
    def __init__(
        self,
        events: list[ev.OpactxEvent],
        value: Any | None = None,
        failed: bool = False,
        schema_path: Path | None = None,
    ):
        self.events = events
        self.value = value
        self.failed = failed
        self.schema_path = schema_path


def _run_stage_fetch_sources(
    project_dir: Path,
    config: Config,
    *,
    fail_fast: bool,
) -> _StageResultWithEvents:
    started = time.perf_counter()
    events: list[ev.OpactxEvent] = [
        ev.StageStarted(command="build", stage_id="fetch_sources", label="Fetch sources")
    ]
    sources: dict[str, Any] = {}
    total = len(config.sources)
    completed = 0
    for source in config.sources:
        note = _source_note(source)
        events.append(
            ev.SourceFetchStarted(
                command="build", name=source.name, type_key=source.type, notes=note
            )
        )
        s_start = time.perf_counter()
        try:
            source_cls = load_source(source.type)
            instance = source_cls(project_dir, **source.with_)
            data = instance.fetch()
            if not _is_json_serializable(data):
                raise ValueError("Source returned non-JSON-serializable data.")
            size_bytes = len(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            sources[source.name] = data
            duration_ms = _elapsed_ms(s_start)
            events.append(
                ev.SourceFetchCompleted(
                    command="build",
                    name=source.name,
                    duration_ms=duration_ms,
                    size_bytes=size_bytes,
                )
            )
            completed += 1
            events.append(
                ev.StageProgress(
                    command="build",
                    stage_id="fetch_sources",
                    current=completed,
                    total=total,
                )
            )
        except Exception as exc:  # noqa: BLE001
            duration_ms = _elapsed_ms(s_start)
            events.append(
                ev.SourceFetchFailed(
                    command="build",
                    name=source.name,
                    duration_ms=duration_ms,
                    message=str(exc),
                    hint="Run with --debug for details.",
                    type_key=source.type,
                    notes=note,
                )
            )
            duration_ms_stage = _elapsed_ms(started)
            events.append(
                ev.StageFailed(
                    command="build",
                    stage_id="fetch_sources",
                    duration_ms=duration_ms_stage,
                    error_code="source_error",
                    message=str(exc),
                )
            )
            return _StageResultWithEvents(events=events, failed=True)
        if not fail_fast and completed != total:
            continue

    duration_ms = _elapsed_ms(started)
    events.append(
        ev.StageCompleted(
            command="build",
            stage_id="fetch_sources",
            duration_ms=duration_ms,
            status="success",
        )
    )
    return _StageResultWithEvents(events=events, value=sources)




def _run_stage_validate_schema(
    project_dir: Path,
    config: Config,
    canonical: dict[str, Any],
) -> _StageResultWithEvents:
    started = time.perf_counter()
    events: list[ev.OpactxEvent] = [
        ev.StageStarted(command="build", stage_id="validate_schema", label="Validate schema")
    ]
    schema_path = Path(config.schema_path)
    if not schema_path.is_absolute():
        schema_path = project_dir / schema_path
    if not schema_path.exists():
        duration_ms = _elapsed_ms(started)
        events.append(ev.SchemaInvalid(command="build", path=schema_path, message="Schema not found."))
        events.append(
            ev.StageFailed(
                command="build",
                stage_id="validate_schema",
                duration_ms=duration_ms,
                error_code="schema_error",
                message=f"Schema not found: {schema_path}",
            )
        )
        return _StageResultWithEvents(events=events, failed=True, schema_path=schema_path)
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        duration_ms = _elapsed_ms(started)
        events.append(ev.SchemaInvalid(command="build", path=schema_path, message=str(exc)))
        events.append(
            ev.StageFailed(
                command="build",
                stage_id="validate_schema",
                duration_ms=duration_ms,
                error_code="schema_error",
                message=f"Invalid JSON schema: {schema_path}",
            )
        )
        return _StageResultWithEvents(events=events, failed=True, schema_path=schema_path)

    events.append(ev.SchemaLoaded(command="build", path=schema_path))
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(canonical), key=lambda err: list(err.path))
    if errors:
        formatted: list[dict[str, str]] = []
        for error in errors:
            if error.path:
                path = "/" + "/".join(str(p) for p in error.path)
            else:
                path = "/"
            formatted.append({"path": path, "message": error.message})
        events.append(
            ev.SchemaValidationFailed(
                command="build",
                schema_path=schema_path,
                errors=formatted,
            )
        )
        duration_ms = _elapsed_ms(started)
        events.append(
            ev.StageFailed(
                command="build",
                stage_id="validate_schema",
                duration_ms=duration_ms,
                error_code="schema_validation",
                message="Schema validation failed.",
            )
        )
        return _StageResultWithEvents(events=events, failed=True, schema_path=schema_path)

    duration_ms = _elapsed_ms(started)
    events.append(ev.SchemaValidationPassed(command="build", schema_path=schema_path))
    events.append(
        ev.StageCompleted(
            command="build",
            stage_id="validate_schema",
            duration_ms=duration_ms,
            status="success",
        )
    )
    return _StageResultWithEvents(events=events, schema_path=schema_path)


def _run_stage_write_bundle(
    output_dir: Path,
    data_bytes: bytes,
    revision: str,
    *,
    clean: bool,
    tarball: bool,
) -> _StageResultWithEvents:
    started = time.perf_counter()
    events: list[ev.OpactxEvent] = [
        ev.StageStarted(command="build", stage_id="write_bundle", label="Write bundle"),
        ev.BundleWriteStarted(command="build", out_dir=output_dir),
    ]
    try:
        if output_dir.exists():
            if output_dir.is_file():
                raise OSError(f"Output path is a file: {output_dir}")
            if clean:
                shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "data.json").write_bytes(data_bytes)
        manifest = {"revision": revision, "roots": ["data.json"]}
        (output_dir / ".manifest").write_bytes(_stable_json_bytes(manifest))
        if tarball:
            _write_tarball(output_dir)
    except OSError as exc:
        duration_ms = _elapsed_ms(started)
        events.append(ev.BundleWriteFailed(command="build", out_dir=output_dir, message=str(exc)))
        events.append(
            ev.StageFailed(
                command="build",
                stage_id="write_bundle",
                duration_ms=duration_ms,
                error_code="write_error",
                message=f"Failed to write bundle: {exc}",
            )
        )
        return _StageResultWithEvents(events=events, failed=True)

    duration_ms = _elapsed_ms(started)
    events.append(
        ev.StageCompleted(
            command="build",
            stage_id="write_bundle",
            duration_ms=duration_ms,
            status="success",
        )
    )
    return _StageResultWithEvents(events=events)


def _resolve_output_dir(project_dir: Path, config: Config, override: Path | None) -> Path:
    output_dir = override or Path(config.output.dir)
    if not output_dir.is_absolute():
        output_dir = project_dir / output_dir
    return output_dir


def _write_tarball(output_dir: Path) -> None:
    tar_path = output_dir.with_suffix(".tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        for path in sorted(output_dir.rglob("*")):
            if path.is_file():
                tar.add(path, arcname=path.relative_to(output_dir))


def _stable_json_bytes(data: dict[str, Any]) -> bytes:
    payload = json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return f"{payload}\n".encode("utf-8")


def _is_json_serializable(data: Any) -> bool:
    try:
        json.dumps(data)
    except TypeError:
        return False
    return True


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000


def _normalize(
    project_dir: Path,
    config: Config,
    intent: dict[str, Any],
    sources: dict[str, Any],
) -> dict[str, Any] | str:
    transforms = list(config.transforms)
    value: dict[str, Any]
    if not transforms:
        value = canonicalize(intent, sources)
    else:
        value = {"intent": intent, "sources": sources}
        for transform in transforms:
            if transform.type == "builtin" and transform.name != "canonicalize":
                return "Only the builtin canonicalize transform is supported in v1."
            try:
                transform_cls = load_transform(transform.type)
                instance = transform_cls(project_dir, **transform.with_)
                value = instance.apply(value)
            except Exception as exc:  # noqa: BLE001
                return str(exc)
        if not isinstance(value, dict):
            return "Transform output must be a mapping."
    if not _is_json_serializable(value):
        return "Canonical context is not JSON-serializable."
    return value


def _source_note(source: Any) -> str | None:
    if source.type == "file":
        path = source.with_.get("path")
        if isinstance(path, str):
            return path
    if source.type == "http":
        url = source.with_.get("url")
        if isinstance(url, str):
            parsed = urlparse(url)
            host = parsed.hostname or ""
            if parsed.port:
                host = f"{host}:{parsed.port}"
            return f"{host}{parsed.path}"
    if source.type == "exec":
        cmd = source.with_.get("cmd")
        if isinstance(cmd, (list, tuple)) and cmd:
            first = str(cmd[0])
            second = str(cmd[1]) if len(cmd) > 1 else ""
            if second:
                return f"{first} {Path(second).name}"
            return first
    return None
