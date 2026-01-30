from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from opactx.config.load import ConfigError, load_config, load_yaml_mapping
from opactx.config.model import Config
from opactx.plugins.registry import load_source, load_transform


@dataclass
class ValidationReport:
    ok: bool = True
    checks: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)


def validate_project(
    project_dir: Path,
    *,
    config_path: Path | None = None,
    strict: bool = False,
    check_schema: bool = True,
    on_step: Callable[[str], None] | None = None,
) -> ValidationReport:
    report = ValidationReport()
    project_dir = project_dir.resolve()

    _emit(on_step, "Loading config...")
    config = _load_config(report, project_dir, config_path)
    if config is None:
        return report
    report.checks["config"] = "ok"

    _emit(on_step, "Validating schema...")
    schema_path = _resolve_schema_path(project_dir, config)
    schema = _load_schema(report, schema_path)
    if schema is None:
        return report
    report.checks["schema"] = "ok"

    _emit(on_step, "Loading intent context...")
    intent = _load_intent(report, project_dir, config)
    if intent is None:
        return report
    report.checks["intent"] = "ok"

    _emit(on_step, "Validating plugins...")
    _validate_plugins(report, config, strict=strict)

    if check_schema:
        _emit(on_step, "Checking schema...")
        _check_schema(report, schema, intent, strict=strict)
    else:
        report.checks["schema_check"] = "skipped"

    report.ok = report.ok and all(status != "failed" for status in report.checks.values())
    return report


def _load_config(
    report: ValidationReport,
    project_dir: Path,
    config_path: Path | None,
) -> Config | None:
    try:
        return load_config(project_dir, config_path)
    except (ConfigError, ValueError) as exc:
        _fail(report, "config", str(exc))
        return None


def _resolve_schema_path(project_dir: Path, config: Config) -> Path:
    schema_path = Path(config.schema_path)
    if not schema_path.is_absolute():
        schema_path = project_dir / schema_path
    return schema_path


def _load_schema(report: ValidationReport, schema_path: Path) -> dict[str, Any] | None:
    if not schema_path.exists():
        _fail(report, "schema", f"Schema not found: {schema_path}", path=str(schema_path))
        return None
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        _fail(report, "schema", f"Invalid JSON schema: {schema_path}")
        return None
    if not isinstance(schema, dict):
        _fail(report, "schema", "Schema must be a JSON object.", path=str(schema_path))
        return None
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        _fail(report, "schema", f"Schema is not valid: {exc.message}")
        return None
    return schema


def _load_intent(
    report: ValidationReport,
    project_dir: Path,
    config: Config,
) -> dict[str, Any] | None:
    context_dir = Path(config.context_dir)
    if not context_dir.is_absolute():
        context_dir = project_dir / context_dir
    try:
        standards = load_yaml_mapping(context_dir / "standards.yaml", required=True)
        exceptions = load_yaml_mapping(context_dir / "exceptions.yaml", required=False)
    except ConfigError as exc:
        _fail(report, "intent", str(exc))
        return None
    return {"standards": standards, "exceptions": exceptions}


def _validate_plugins(
    report: ValidationReport,
    config: Config,
    *,
    strict: bool,
) -> None:
    errors: list[str] = []
    warnings: list[str] = []

    for source in config.sources:
        if not source.type:
            errors.append(f"Source '{source.name}' has an empty type.")
            continue
        if strict:
            try:
                load_source(source.type)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Unknown source type: {source.type}")
        else:
            try:
                load_source(source.type)
            except Exception:
                warnings.append(f"Source type not resolved: {source.type}")

    transforms = list(config.transforms)
    if not transforms:
        if strict:
            try:
                load_transform("builtin")
            except Exception:
                errors.append("Default builtin transform is not available.")
    else:
        for transform in transforms:
            if not transform.type:
                errors.append(f"Transform '{transform.name}' has an empty type.")
                continue
            if strict:
                try:
                    load_transform(transform.type)
                except Exception:  # noqa: BLE001
                    errors.append(f"Unknown transform type: {transform.type}")
            else:
                try:
                    load_transform(transform.type)
                except Exception:
                    warnings.append(f"Transform type not resolved: {transform.type}")

    if errors:
        _fail(report, "plugins", "\n".join(errors))
    else:
        report.checks["plugins"] = "ok" if strict else "skipped"

    report.warnings.extend(warnings)


def _check_schema(
    report: ValidationReport,
    schema: dict[str, Any],
    intent: dict[str, Any],
    *,
    strict: bool,
) -> None:
    candidate = {"standards": intent["standards"], "exceptions": intent["exceptions"], "sources": {}}
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(candidate), key=lambda err: list(err.path))
    if not errors:
        report.checks["schema_check"] = "ok"
        return

    non_source_errors: list[str] = []
    source_errors: list[str] = []
    for error in errors:
        path = list(error.path)
        pointer = "/" + "/".join(str(p) for p in path) if path else "/"
        message = f"{pointer}: {error.message}"
        if path and path[0] == "sources":
            source_errors.append(message)
        else:
            non_source_errors.append(message)

    if non_source_errors:
        _fail(report, "schema_check", _format_schema_errors(non_source_errors))
        return

    warning = "Schema requires data from sources; build may succeed only when sources are fetched."
    if strict:
        _fail(report, "schema_check", warning)
    else:
        report.checks["schema_check"] = "partial"
        report.warnings.append(warning)


def _format_schema_errors(lines: Iterable[str]) -> str:
    entries = list(lines)
    if len(entries) <= 20:
        return "Schema validation failed:\n" + "\n".join(f"- {line}" for line in entries)
    head = entries[:20]
    return "Schema validation failed:\n" + "\n".join(f"- {line}" for line in head) + f"\n...and {len(entries) - 20} more"


def _fail(report: ValidationReport, check: str, message: str, path: str | None = None) -> None:
    report.ok = False
    report.checks[check] = "failed"
    error = {"check": check, "message": message}
    if path:
        error["path"] = path
    report.errors.append(error)


def _emit(callback: Callable[[str], None] | None, message: str) -> None:
    if callback:
        callback(message)
