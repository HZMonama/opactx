from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from jsonschema import Draft202012Validator

from opactx.config.load import ConfigError, load_config, load_yaml_mapping
from opactx.config.model import Config
from opactx.core import events as ev
from opactx.plugins.registry import load_source, load_transform
from opactx.schema import SchemaLoadError, load_compiled_schema
from opactx.transforms.builtin import canonicalize, is_builtin_transform


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
    schema = _load_schema_raw(project_dir, schema_path)
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

    schema_result = _schema_check_events(
        project_dir,
        config,
        schema_path,
        schema,
        intent,
        strict=strict,
    )
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


def _load_schema_raw(project_dir: Path, schema_path: Path) -> dict[str, Any] | str:
    try:
        return load_compiled_schema(
            project_dir=project_dir,
            schema_path=schema_path,
            emit_compiled_artifact=False,
        )
    except SchemaLoadError as exc:
        return str(exc)


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
                continue
            if transform.type == "builtin" and not is_builtin_transform(transform.name):
                message = f"Unknown builtin transform name: {transform.name}"
                if strict:
                    errors.append(message)
                else:
                    events.append(
                        ev.Warning(
                            command="validate",
                            code="plugin_missing",
                            message=message,
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


@dataclass
class _SourceInfluence:
    root: bool
    top_fields: set[str]


class _CandidateBuildResult:
    def __init__(
        self,
        *,
        candidate: dict[str, Any] | None,
        failed: bool,
        message: str = "",
        incomplete: bool = False,
        incomplete_reason: str = "",
        source_influence: _SourceInfluence | None = None,
    ):
        self.candidate = candidate
        self.failed = failed
        self.message = message
        self.incomplete = incomplete
        self.incomplete_reason = incomplete_reason
        self.source_influence = source_influence or _SourceInfluence(root=False, top_fields=set())


def _schema_check_events(
    project_dir: Path,
    config: Config,
    schema_path: Path,
    schema: dict[str, Any],
    intent: dict[str, Any],
    *,
    strict: bool,
) -> _SchemaCheckResult:
    candidate_result = _build_candidate_context(
        project_dir,
        config,
        intent,
        strict=strict,
    )
    if candidate_result.failed or candidate_result.candidate is None:
        return _SchemaCheckResult(
            events=[],
            failed=True,
            status="failed",
            message=candidate_result.message or "Failed to assemble schema candidate context.",
        )

    candidate = candidate_result.candidate
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

    source_related = _schema_errors_require_source_data(
        errors,
        source_influence=candidate_result.source_influence,
    )
    if candidate_result.incomplete:
        source_related = True

    if not source_related:
        events: list[ev.OpactxEvent] = [
            ev.SchemaValidationFailed(command="validate", schema_path=schema_path, errors=formatted)
        ]
        return _SchemaCheckResult(
            events=events,
            failed=True,
            status="failed",
            message="Schema validation failed.",
        )

    warning = candidate_result.incomplete_reason or (
        "Schema requires data from sources; build may succeed only when sources are fetched."
    )
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


def _build_candidate_context(
    project_dir: Path,
    config: Config,
    intent: dict[str, Any],
    *,
    strict: bool,
) -> _CandidateBuildResult:
    source_influence = _detect_source_influence(config)
    placeholder_sources: dict[str, Any] = {source.name: {} for source in config.sources}
    value: dict[str, Any] = canonicalize(intent, placeholder_sources)

    for transform in config.transforms:
        try:
            transform_cls = load_transform(transform.type)
        except Exception:  # noqa: BLE001
            if strict:
                return _CandidateBuildResult(
                    candidate=None,
                    failed=True,
                    message=f"Transform type not resolved during schema preflight: {transform.type}",
                    source_influence=source_influence,
                )
            return _CandidateBuildResult(
                candidate=value,
                failed=False,
                incomplete=True,
                incomplete_reason=(
                    "Schema preflight skipped one or more unresolved transforms; "
                    "run `opactx validate --strict` to enforce plugin resolution."
                ),
                source_influence=source_influence,
            )

        try:
            if transform.type == "builtin":
                instance = transform_cls(
                    project_dir,
                    transform_name=transform.name,
                    intent=intent,
                    sources=placeholder_sources,
                    schema_path=config.schema_path,
                    **transform.with_,
                )
            else:
                instance = transform_cls(project_dir, **transform.with_)
            value = instance.apply(value)
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            if not strict and _transform_error_likely_source_dependent(transform, message):
                return _CandidateBuildResult(
                    candidate=value,
                    failed=False,
                    incomplete=True,
                    incomplete_reason=(
                        "Schema preflight could not fully evaluate source-dependent transforms "
                        "without fetching sources."
                    ),
                    source_influence=source_influence,
                )
            return _CandidateBuildResult(
                candidate=None,
                failed=True,
                message=f"Failed to evaluate transform '{transform.name}' during schema preflight: {message}",
                source_influence=source_influence,
            )

        if not isinstance(value, dict):
            return _CandidateBuildResult(
                candidate=None,
                failed=True,
                message="Transform output must be a mapping.",
                source_influence=source_influence,
            )

    if not _is_json_serializable(value):
        return _CandidateBuildResult(
            candidate=None,
            failed=True,
            message="Schema candidate context is not JSON-serializable.",
            source_influence=source_influence,
        )

    return _CandidateBuildResult(
        candidate=value,
        failed=False,
        source_influence=source_influence,
    )


def _detect_source_influence(config: Config) -> _SourceInfluence:
    top_fields: set[str] = {"sources"}
    root = False

    for transform in config.transforms:
        source_dependent = _transform_references_sources(transform)
        if transform.type == "builtin" and transform.name == "canonicalize":
            source_dependent = True
        if not source_dependent:
            continue
        target = _transform_target_path(transform)
        if target is None:
            root = True
            continue
        parts = _parse_context_path_for_validate(target)
        if parts is None:
            root = True
            continue
        if not parts:
            root = True
            continue
        top_fields.add(parts[0])

    return _SourceInfluence(root=root, top_fields=top_fields)


def _transform_target_path(transform: Any) -> str | None:
    if transform.type != "builtin":
        return None
    options = transform.with_
    if not isinstance(options, dict):
        return None
    if transform.name in {"mount", "merge", "pick"}:
        target = options.get("target")
        if isinstance(target, str):
            return target
    return None


def _parse_context_path_for_validate(path: str) -> list[str] | None:
    text = path.strip()
    if text == "context":
        return []
    if not text.startswith("context."):
        return None
    parts = text[len("context.") :].split(".")
    if any(not part for part in parts):
        return None
    return parts


def _transform_references_sources(transform: Any) -> bool:
    if transform.type == "builtin" and transform.name == "mount":
        source_id = transform.with_.get("source_id")
        if isinstance(source_id, str) and source_id.strip():
            return True
    return _contains_source_reference(transform.with_)


def _contains_source_reference(value: Any) -> bool:
    if isinstance(value, str):
        text = value.strip()
        return (
            text == "sources"
            or text.startswith("sources.")
            or text == "context.sources"
            or text.startswith("context.sources.")
        )
    if isinstance(value, list):
        return any(_contains_source_reference(item) for item in value)
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "source_id":
                if isinstance(item, str) and item.strip():
                    return True
            if _contains_source_reference(item):
                return True
    return False


def _transform_error_likely_source_dependent(transform: Any, message: str) -> bool:
    lowered = message.lower()
    if "mount source not found" in lowered:
        return True
    if "sources." in lowered and "not found" in lowered:
        return True
    if _transform_references_sources(transform):
        return True
    return False


def _schema_errors_require_source_data(
    errors: list[Any],
    *,
    source_influence: _SourceInfluence,
) -> bool:
    if not errors:
        return False
    for error in errors:
        if not _is_source_related_schema_error(error, source_influence=source_influence):
            return False
    return True


def _is_source_related_schema_error(error: Any, *, source_influence: _SourceInfluence) -> bool:
    path_parts = [str(part) for part in error.path]
    if path_parts:
        top = path_parts[0]
        if top == "sources":
            return True
        if top in source_influence.top_fields:
            return True
        return False

    message = str(error.message)
    missing = _extract_required_property_name(message)
    if missing is not None:
        if missing in source_influence.top_fields:
            return True
        if source_influence.root:
            return True
    if source_influence.root and "Additional properties are not allowed" in message:
        return True
    return False


def _extract_required_property_name(message: str) -> str | None:
    suffix = "' is a required property"
    if not message.endswith(suffix):
        return None
    if not message.startswith("'"):
        return None
    end_index = message.find("'", 1)
    if end_index <= 1:
        return None
    return message[1:end_index]


def _is_json_serializable(data: Any) -> bool:
    try:
        json.dumps(data)
    except TypeError:
        return False
    return True


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000
