from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from opactx.config.load import ConfigError, load_config, load_yaml_mapping
from opactx.config.model import Config
from opactx.core import events as ev
from opactx.plugins.registry import load_source, load_transform


def validate_events(
    project_dir: Path,
    *,
    config_path: Path | None = None,
    strict: bool = False,
    check_schema: bool = True,
    debug: bool = False,
) -> Iterable[ev.OpactxEvent]:
    project_dir = project_dir.resolve()
    config_path = config_path or Path("opactx.yaml")
    if not config_path.is_absolute():
        config_path = project_dir / config_path

    yield ev.CommandStarted(
        command="validate",
        project_dir=project_dir,
        config_path=config_path,
        options={"strict": strict, "check_schema": check_schema},
    )

    yield ev.StageStarted(command="validate", stage_id="load_config", label="Load config")
    started = time.perf_counter()
    try:
        config = load_config(project_dir, config_path)
    except (ConfigError, ValueError) as exc:
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="validate",
            stage_id="load_config",
            duration_ms=duration_ms,
            error_code="config_error",
            message=str(exc),
        )
        yield ev.CommandCompleted(command="validate", ok=False, exit_code=2)
        return
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="validate",
        stage_id="load_config",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.StageStarted(command="validate", stage_id="load_schema", label="Load schema")
    started = time.perf_counter()
    schema_path = _resolve_schema_path(project_dir, config)
    schema = _load_schema_raw(schema_path)
    if isinstance(schema, str):
        duration_ms = _elapsed_ms(started)
        yield ev.SchemaInvalid(command="validate", path=schema_path, message=schema)
        yield ev.StageFailed(
            command="validate",
            stage_id="load_schema",
            duration_ms=duration_ms,
            error_code="schema_error",
            message=schema,
        )
        yield ev.CommandCompleted(command="validate", ok=False, exit_code=2)
        return
    yield ev.SchemaLoaded(command="validate", path=schema_path)
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="validate",
        stage_id="load_schema",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.StageStarted(command="validate", stage_id="load_intent", label="Load intent context")
    started = time.perf_counter()
    intent = _load_intent_values(project_dir, config)
    if isinstance(intent, str):
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="validate",
            stage_id="load_intent",
            duration_ms=duration_ms,
            error_code="intent_error",
            message=intent,
        )
        yield ev.CommandCompleted(command="validate", ok=False, exit_code=2)
        return
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="validate",
        stage_id="load_intent",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.StageStarted(command="validate", stage_id="resolve_plugins", label="Resolve plugins")
    started = time.perf_counter()
    plugin_result = _validate_plugins_events(config, strict=strict)
    for item in plugin_result.events:
        yield item
    if plugin_result.failed:
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="validate",
            stage_id="resolve_plugins",
            duration_ms=duration_ms,
            error_code="plugin_error",
            message=plugin_result.message,
        )
        yield ev.CommandCompleted(command="validate", ok=False, exit_code=2)
        return
    duration_ms = _elapsed_ms(started)
    status = "success" if strict else "skipped"
    yield ev.StageCompleted(
        command="validate",
        stage_id="resolve_plugins",
        duration_ms=duration_ms,
        status=status,
    )

    yield ev.StageStarted(command="validate", stage_id="schema_check", label="Schema check")
    started = time.perf_counter()
    if not check_schema:
        duration_ms = _elapsed_ms(started)
        yield ev.StageCompleted(
            command="validate",
            stage_id="schema_check",
            duration_ms=duration_ms,
            status="skipped",
        )
        yield ev.CommandCompleted(command="validate", ok=True, exit_code=0)
        return

    schema_result = _schema_check_events(schema_path, schema, intent, strict=strict)
    for item in schema_result.events:
        yield item
    duration_ms = _elapsed_ms(started)
    if schema_result.failed:
        yield ev.StageFailed(
            command="validate",
            stage_id="schema_check",
            duration_ms=duration_ms,
            error_code="schema_validation",
            message=schema_result.message,
        )
        yield ev.CommandCompleted(command="validate", ok=False, exit_code=2)
        return

    yield ev.StageCompleted(
        command="validate",
        stage_id="schema_check",
        duration_ms=duration_ms,
        status=schema_result.status,
    )
    yield ev.CommandCompleted(command="validate", ok=True, exit_code=0)


def _resolve_schema_path(project_dir: Path, config: Config) -> Path:
    schema_path = Path(config.schema_path)
    if not schema_path.is_absolute():
        schema_path = project_dir / schema_path
    return schema_path


def _load_schema_raw(schema_path: Path) -> dict[str, Any] | str:
    if not schema_path.exists():
        return f"Schema not found: {schema_path}"
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return f"Invalid JSON schema: {schema_path}"
    if not isinstance(schema, dict):
        return "Schema must be a JSON object."
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        return f"Schema is not valid: {exc.message}"
    return schema


def _load_intent_values(project_dir: Path, config: Config) -> dict[str, Any] | str:
    context_dir = Path(config.context_dir)
    if not context_dir.is_absolute():
        context_dir = project_dir / context_dir
    try:
        standards = load_yaml_mapping(context_dir / "standards.yaml", required=True)
        exceptions = load_yaml_mapping(context_dir / "exceptions.yaml", required=False)
    except ConfigError as exc:
        return str(exc)
    return {"standards": standards, "exceptions": exceptions}


class _PluginCheckResult:
    def __init__(self, events: list[ev.OpactxEvent], failed: bool, message: str = ""):
        self.events = events
        self.failed = failed
        self.message = message


def _validate_plugins_events(config: Config, *, strict: bool) -> _PluginCheckResult:
    events: list[ev.OpactxEvent] = []
    errors: list[str] = []
    for source in config.sources:
        if not source.type:
            errors.append(f"Source '{source.name}' has an empty type.")
            continue
        try:
            plugin = load_source(source.type)
            impl = f"{plugin.__module__}:{plugin.__name__}"
            events.append(
                ev.PluginResolved(
                    command="validate", kind="source", type_key=source.type, impl=impl
                )
            )
        except Exception:  # noqa: BLE001
            events.append(
                ev.PluginMissing(command="validate", kind="source", type_key=source.type)
            )
            if strict:
                errors.append(f"Unknown source type: {source.type}")
            else:
                events.append(
                    ev.Warning(
                        command="validate",
                        code="plugin_missing",
                        message=f"Source type not resolved: {source.type}",
                    )
                )

    transforms = list(config.transforms)
    if not transforms:
        if strict:
            try:
                plugin = load_transform("builtin")
                impl = f"{plugin.__module__}:{plugin.__name__}"
                events.append(
                    ev.PluginResolved(command="validate", kind="transform", type_key="builtin", impl=impl)
                )
            except Exception:  # noqa: BLE001
                errors.append("Default builtin transform is not available.")
    else:
        for transform in transforms:
            if not transform.type:
                errors.append(f"Transform '{transform.name}' has an empty type.")
                continue
            try:
                plugin = load_transform(transform.type)
                impl = f"{plugin.__module__}:{plugin.__name__}"
                events.append(
                    ev.PluginResolved(
                        command="validate", kind="transform", type_key=transform.type, impl=impl
                    )
                )
            except Exception:  # noqa: BLE001
                events.append(
                    ev.PluginMissing(command="validate", kind="transform", type_key=transform.type)
                )
                if strict:
                    errors.append(f"Unknown transform type: {transform.type}")
                else:
                    events.append(
                        ev.Warning(
                            command="validate",
                            code="plugin_missing",
                            message=f"Transform type not resolved: {transform.type}",
                        )
                    )

    if errors:
        return _PluginCheckResult(events=events, failed=True, message="\n".join(errors))
    return _PluginCheckResult(events=events, failed=False)


class _SchemaCheckResult:
    def __init__(self, events: list[ev.OpactxEvent], failed: bool, status: str, message: str):
        self.events = events
        self.failed = failed
        self.status = status
        self.message = message


def _schema_check_events(
    schema_path: Path,
    schema: dict[str, Any],
    intent: dict[str, Any],
    *,
    strict: bool,
) -> _SchemaCheckResult:
    candidate = {"standards": intent["standards"], "exceptions": intent["exceptions"], "sources": {}}
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(candidate), key=lambda err: list(err.path))
    if not errors:
        return _SchemaCheckResult(
            events=[ev.SchemaValidationPassed(command="validate", schema_path=schema_path)],
            failed=False,
            status="success",
            message="",
        )

    formatted: list[dict[str, str]] = []
    non_source = False
    for error in errors:
        if error.path:
            path = "/" + "/".join(str(p) for p in error.path)
        else:
            path = "/"
        formatted.append({"path": path, "message": error.message})
        if not (error.path and list(error.path)[0] == "sources"):
            non_source = True

    if non_source:
        events: list[ev.OpactxEvent] = [
            ev.SchemaValidationFailed(command="validate", schema_path=schema_path, errors=formatted)
        ]
        return _SchemaCheckResult(
            events=events,
            failed=True,
            status="failed",
            message="Schema validation failed.",
        )

    warning = "Schema requires data from sources; build may succeed only when sources are fetched."
    if strict:
        events = [ev.SchemaValidationFailed(command="validate", schema_path=schema_path, errors=formatted)]
        return _SchemaCheckResult(
            events=events,
            failed=True,
            status="failed",
            message=warning,
        )

    events = [
        ev.Warning(command="validate", code="schema_partial", message=warning),
    ]
    return _SchemaCheckResult(
        events=events,
        failed=False,
        status="partial",
        message="",
    )


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000
