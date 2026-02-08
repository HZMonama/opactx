from __future__ import annotations

import copy
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from jsonschema import Draft202012Validator
from opactx.schema import SchemaLoadError, load_compiled_schema


def canonicalize(intent: dict[str, Any], sources: dict[str, Any]) -> dict[str, Any]:
    return {
        "standards": intent.get("standards", {}),
        "exceptions": intent.get("exceptions", {}),
        "sources": sources,
    }


_MISSING = object()

BUILTIN_TRANSFORMS = {
    "canonicalize",
    "mount",
    "merge",
    "pick",
    "rename",
    "coerce",
    "defaults",
    "validate_schema",
    "ref_resolve",
    "sort_stable",
    "dedupe",
}


def is_builtin_transform(name: str) -> bool:
    return name in BUILTIN_TRANSFORMS


class BuiltinTransform:
    def __init__(
        self,
        project_dir: Path | str,
        *,
        transform_name: str | None = None,
        intent: dict[str, Any] | None = None,
        sources: dict[str, Any] | None = None,
        schema_path: str | Path | None = None,
        **options: Any,
    ):
        self.project_dir = Path(project_dir)
        self.transform_name = transform_name or "canonicalize"
        self.intent = intent or {}
        self.sources = sources or {}
        self.options = options
        self.schema_path = Path(schema_path) if schema_path else None

    def apply(self, value: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError("Transform input must be a mapping.")
        if self.transform_name == "canonicalize":
            return canonicalize(self.intent, self.sources)

        handler = _HANDLERS.get(self.transform_name)
        if handler is None:
            raise ValueError(f"Unknown builtin transform: {self.transform_name}")
        return handler(
            value,
            self.options,
            intent=self.intent,
            sources=self.sources,
            project_dir=self.project_dir,
            schema_path=self.schema_path,
        )


class CanonicalizeTransform(BuiltinTransform):
    """Backward-compatible alias for older plugin entrypoints."""

    def __init__(self, project_dir: Path | str, **options: Any):
        super().__init__(project_dir, transform_name="canonicalize", **options)


def _mount(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del intent, project_dir, schema_path
    source_id = _require_string(options, "source_id")
    target = _require_context_path(options, "target")
    strategy = str(options.get("strategy", "merge")).strip().lower()
    source_value = _resolve_source_value(context, sources, source_id)

    existing = _get_context_value(context, target, default=_MISSING)
    if existing is _MISSING:
        merged = _clone(source_value)
    elif strategy in {"merge", "deep"}:
        merged = _deep_merge(existing, source_value)
    elif strategy == "replace":
        merged = _clone(source_value)
    else:
        raise ValueError(
            "mount strategy must be one of: merge, deep, replace."
        )
    _set_context_value(context, target, merged)
    return context


def _merge(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del project_dir, schema_path
    target = _require_context_path(options, "target")
    items = options.get("from")
    if items is None:
        items = options.get("inputs")
    if items is None:
        items = options.get("objects")
    if not isinstance(items, list) or not items:
        raise ValueError("merge requires a non-empty list in with.from (or with.inputs).")

    include_existing = bool(options.get("include_existing", False))
    merged: Any = {}
    existing = _get_context_value(context, target, default=_MISSING)
    if include_existing and existing is not _MISSING:
        merged = _clone(existing)

    for item in items:
        current = _resolve_item_reference(item, context, intent, sources)
        merged = _deep_merge(merged, current)

    _set_context_value(context, target, merged)
    return context


def _pick(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del intent, sources, project_dir, schema_path
    path = _require_context_path(options, "path")
    target = _parse_context_path(str(options.get("target", options["path"])))
    keys = options.get("keys")
    if not isinstance(keys, list) or not all(isinstance(key, str) for key in keys):
        raise ValueError("pick requires with.keys as a list of strings.")
    strict = bool(options.get("strict", False))

    value = _get_context_value(context, path, default=_MISSING)
    if not isinstance(value, dict):
        raise ValueError("pick path must point to an object.")

    picked: dict[str, Any] = {}
    for key in keys:
        if key in value:
            picked[key] = _clone(value[key])
        elif strict:
            raise ValueError(f"pick key not found: {key}")

    _set_context_value(context, target, picked)
    return context


def _rename(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del intent, sources, project_dir, schema_path
    moves = options.get("moves")
    if moves is None:
        source_path = options.get("from")
        target_path = options.get("to")
        if not isinstance(source_path, str) or not isinstance(target_path, str):
            raise ValueError("rename requires with.moves or with.from + with.to.")
        moves = [{"from": source_path, "to": target_path}]
    if not isinstance(moves, list):
        raise ValueError("rename with.moves must be a list.")

    default_ignore_missing = bool(options.get("ignore_missing", True))
    for move in moves:
        if not isinstance(move, dict):
            raise ValueError("rename move entries must be mappings.")
        source = _parse_context_path(_require_string(move, "from"))
        target = _parse_context_path(_require_string(move, "to"))
        ignore_missing = bool(move.get("ignore_missing", default_ignore_missing))
        moved = _pop_context_value(context, source)
        if moved is _MISSING:
            if ignore_missing:
                continue
            raise ValueError(f"rename source path not found: {_format_context_path(source)}")
        _set_context_value(context, target, moved)
    return context


def _coerce(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del intent, sources, project_dir, schema_path
    rules = _extract_rules(options, required={"path", "type"}, transform_name="coerce")
    default_ignore_missing = bool(options.get("ignore_missing", True))
    for rule in rules:
        path = _parse_context_path(_require_string(rule, "path"))
        type_name = str(rule["type"]).strip().lower()
        ignore_missing = bool(rule.get("ignore_missing", default_ignore_missing))
        current = _get_context_value(context, path, default=_MISSING)
        if current is _MISSING:
            if ignore_missing:
                continue
            raise ValueError(f"coerce path not found: {_format_context_path(path)}")
        coerced = _coerce_value(current, type_name)
        _set_context_value(context, path, coerced)
    return context


def _defaults(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del intent, sources, project_dir, schema_path
    assignments = list(_iter_default_assignments(options))
    if not assignments:
        raise ValueError("defaults requires with.values, with.rules, or with.path + with.value.")
    for path_raw, value in assignments:
        path = _parse_context_path(path_raw)
        current = _get_context_value(context, path, default=_MISSING)
        if current is _MISSING:
            _set_context_value(context, path, _clone(value))
    return context


def _validate_schema(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del intent, sources
    path_value = options.get("schema")
    if isinstance(path_value, str):
        schema_file = Path(path_value)
    elif schema_path is not None:
        schema_file = schema_path
    else:
        raise ValueError("validate_schema requires schema path in config or with.schema.")
    try:
        schema = load_compiled_schema(
            project_dir=project_dir,
            schema_path=schema_file,
            emit_compiled_artifact=False,
        )
    except SchemaLoadError as exc:
        raise ValueError(str(exc)) from exc

    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(context), key=lambda err: list(err.path))
    if errors:
        rendered: list[str] = []
        for error in errors[:5]:
            if error.path:
                path = "/" + "/".join(str(part) for part in error.path)
            else:
                path = "/"
            rendered.append(f"{path}: {error.message}")
        raise ValueError("Schema validation failed: " + "; ".join(rendered))
    return context


def _ref_resolve(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del intent, sources, project_dir, schema_path
    rules = _extract_rules(
        options,
        required={"items", "lookup", "ref_key", "target_key"},
        transform_name="ref_resolve",
    )
    for rule in rules:
        items_path = _parse_context_path(_require_string(rule, "items"))
        lookup_path = _parse_context_path(_require_string(rule, "lookup"))
        ref_key = _split_relative_path(_require_string(rule, "ref_key"))
        target_key = _split_relative_path(_require_string(rule, "target_key"))
        required = bool(rule.get("required", False))
        copy_value = bool(rule.get("copy", True))

        items = _get_context_value(context, items_path, default=_MISSING)
        lookup = _get_context_value(context, lookup_path, default=_MISSING)
        if not isinstance(items, list):
            raise ValueError("ref_resolve items path must point to an array.")
        if not isinstance(lookup, dict):
            raise ValueError("ref_resolve lookup path must point to an object map.")

        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise ValueError(f"ref_resolve expected object item at index {index}.")
            ref = _get_relative_value(item, ref_key, default=_MISSING)
            if ref is _MISSING or ref is None:
                if required:
                    raise ValueError(f"ref_resolve missing ref key on item index {index}.")
                continue
            resolved = lookup.get(ref, _MISSING)
            if resolved is _MISSING:
                if required:
                    raise ValueError(f"ref_resolve missing lookup value for key: {ref}")
                continue
            _set_relative_value(item, target_key, _clone(resolved) if copy_value else resolved)
    return context


def _sort_stable(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del intent, sources, project_dir, schema_path
    rules = _extract_rules(options, required={"path"}, transform_name="sort_stable")
    for rule in rules:
        path = _parse_context_path(_require_string(rule, "path"))
        order = str(rule.get("order", rule.get("direction", "asc"))).strip().lower()
        if order not in {"asc", "desc"}:
            raise ValueError("sort_stable order must be 'asc' or 'desc'.")
        key_path = rule.get("by")
        if key_path is not None and not isinstance(key_path, str):
            raise ValueError("sort_stable with.by must be a string when provided.")
        key_parts = _split_relative_path(key_path) if isinstance(key_path, str) else None

        values = _get_context_value(context, path, default=_MISSING)
        if not isinstance(values, list):
            raise ValueError("sort_stable path must point to an array.")
        sorted_values = _stable_sorted(values, key_parts, reverse=(order == "desc"))
        _set_context_value(context, path, sorted_values)
    return context


def _dedupe(
    context: dict[str, Any],
    options: dict[str, Any],
    *,
    intent: dict[str, Any],
    sources: dict[str, Any],
    project_dir: Path,
    schema_path: Path | None,
) -> dict[str, Any]:
    del intent, sources, project_dir, schema_path
    rules = _extract_rules(options, required={"path"}, transform_name="dedupe")
    for rule in rules:
        path = _parse_context_path(_require_string(rule, "path"))
        key_path = rule.get("by")
        if key_path is not None and not isinstance(key_path, str):
            raise ValueError("dedupe with.by must be a string when provided.")
        keep = str(rule.get("keep", "first")).strip().lower()
        if keep not in {"first", "last"}:
            raise ValueError("dedupe keep must be one of: first, last.")

        values = _get_context_value(context, path, default=_MISSING)
        if not isinstance(values, list):
            raise ValueError("dedupe path must point to an array.")
        key_parts = _split_relative_path(key_path) if isinstance(key_path, str) else None

        if keep == "first":
            deduped = _dedupe_first(values, key_parts)
        else:
            deduped = list(reversed(_dedupe_first(list(reversed(values)), key_parts)))
        _set_context_value(context, path, deduped)
    return context


_HANDLERS: dict[str, Callable[..., dict[str, Any]]] = {
    "mount": _mount,
    "merge": _merge,
    "pick": _pick,
    "rename": _rename,
    "coerce": _coerce,
    "defaults": _defaults,
    "validate_schema": _validate_schema,
    "ref_resolve": _ref_resolve,
    "sort_stable": _sort_stable,
    "dedupe": _dedupe,
}


def _require_string(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing required option: {key}")
    return value.strip()


def _require_context_path(mapping: dict[str, Any], key: str) -> list[str]:
    return _parse_context_path(_require_string(mapping, key))


def _parse_context_path(path: str) -> list[str]:
    text = path.strip()
    if text == "context":
        return []
    if text.startswith("context."):
        text = text[len("context.") :]
    else:
        raise ValueError(f"Path must start with 'context': {path}")
    if text == "":
        return []
    parts = text.split(".")
    if any(not part for part in parts):
        raise ValueError(f"Invalid context path: {path}")
    return parts


def _split_relative_path(path: str) -> list[str]:
    text = path.strip()
    if text == "":
        return []
    parts = text.split(".")
    if any(not part for part in parts):
        raise ValueError(f"Invalid path: {path}")
    return parts


def _get_context_value(data: dict[str, Any], path: list[str], *, default: Any = _MISSING) -> Any:
    current: Any = data
    for part in path:
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def _set_context_value(data: dict[str, Any], path: list[str], value: Any) -> None:
    if not path:
        if not isinstance(value, dict):
            raise ValueError("context root replacement must be an object.")
        data.clear()
        data.update(_clone(value))
        return
    current = data
    for part in path[:-1]:
        next_value = current.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            current[part] = next_value
        current = next_value
    current[path[-1]] = _clone(value)


def _pop_context_value(data: dict[str, Any], path: list[str]) -> Any:
    if not path:
        return _MISSING
    current = data
    for part in path[:-1]:
        if not isinstance(current, dict) or part not in current:
            return _MISSING
        next_value = current[part]
        if not isinstance(next_value, dict):
            return _MISSING
        current = next_value
    if not isinstance(current, dict) or path[-1] not in current:
        return _MISSING
    return current.pop(path[-1])


def _resolve_source_value(context: dict[str, Any], sources: dict[str, Any], source_id: str) -> Any:
    if source_id in sources:
        return _clone(sources[source_id])
    context_sources = context.get("sources")
    if isinstance(context_sources, dict) and source_id in context_sources:
        return _clone(context_sources[source_id])
    raise ValueError(f"mount source not found: {source_id}")


def _resolve_item_reference(
    item: Any,
    context: dict[str, Any],
    intent: dict[str, Any],
    sources: dict[str, Any],
) -> Any:
    if isinstance(item, dict) and "path" in item and isinstance(item["path"], str):
        return _resolve_reference_path(item["path"], context, intent, sources)
    if isinstance(item, str):
        text = item.strip()
        if text == "context" or text.startswith("context."):
            return _resolve_reference_path(text, context, intent, sources)
        if text == "intent" or text.startswith("intent."):
            return _resolve_reference_path(text, context, intent, sources)
        if text == "sources" or text.startswith("sources."):
            return _resolve_reference_path(text, context, intent, sources)
    return _clone(item)


def _resolve_reference_path(
    path: str,
    context: dict[str, Any],
    intent: dict[str, Any],
    sources: dict[str, Any],
) -> Any:
    text = path.strip()
    if text == "context":
        return _clone(context)
    if text.startswith("context."):
        value = _get_context_value(context, _parse_context_path(text), default=_MISSING)
        if value is _MISSING:
            raise ValueError(f"merge input path not found: {path}")
        return _clone(value)
    if text == "sources":
        return _clone(sources)
    if text.startswith("sources."):
        value = _get_relative_value(sources, _split_relative_path(text[len("sources.") :]), default=_MISSING)
        if value is _MISSING:
            raise ValueError(f"merge input path not found: {path}")
        return _clone(value)
    if text == "intent":
        return _clone(intent)
    if text.startswith("intent."):
        value = _get_relative_value(intent, _split_relative_path(text[len("intent.") :]), default=_MISSING)
        if value is _MISSING:
            raise ValueError(f"merge input path not found: {path}")
        return _clone(value)
    raise ValueError(f"Unsupported reference path: {path}")


def _extract_rules(
    options: dict[str, Any],
    *,
    required: set[str],
    transform_name: str,
) -> list[dict[str, Any]]:
    rules = options.get("rules")
    if rules is None:
        rule = {key: options[key] for key in required if key in options}
        if required.issubset(rule):
            rules = [dict(options)]
        else:
            raise ValueError(f"{transform_name} requires with.rules or required top-level rule keys.")
    if not isinstance(rules, list) or not rules:
        raise ValueError(f"{transform_name} rules must be a non-empty list.")
    normalized: list[dict[str, Any]] = []
    for rule in rules:
        if not isinstance(rule, dict):
            raise ValueError(f"{transform_name} rules entries must be mappings.")
        missing = [key for key in required if key not in rule]
        if missing:
            raise ValueError(f"{transform_name} rule missing keys: {', '.join(sorted(missing))}")
        normalized.append(rule)
    return normalized


def _iter_default_assignments(options: dict[str, Any]) -> list[tuple[str, Any]]:
    values = options.get("values")
    if isinstance(values, dict):
        return [(str(path), value) for path, value in values.items()]

    rules = options.get("rules")
    if isinstance(rules, list):
        assignments: list[tuple[str, Any]] = []
        for rule in rules:
            if not isinstance(rule, dict):
                raise ValueError("defaults rules entries must be mappings.")
            path = _require_string(rule, "path")
            if "value" not in rule:
                raise ValueError("defaults rule missing key: value")
            assignments.append((path, rule["value"]))
        return assignments

    if "path" in options and "value" in options:
        return [(_require_string(options, "path"), options["value"])]
    return []


def _coerce_value(value: Any, type_name: str) -> Any:
    if type_name in {"bool", "boolean"}:
        return _to_bool(value)
    if type_name in {"int", "integer"}:
        return _to_int(value)
    if type_name in {"float", "number"}:
        return _to_float(value)
    if type_name == "string":
        return str(value)
    if type_name == "timestamp":
        return _to_rfc3339(value)
    raise ValueError(f"Unsupported coerce type: {type_name}")


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False
        raise ValueError(f"Cannot coerce to bool: {value!r}")
    if isinstance(value, str):
        normalized = value.strip().lower()
        truthy = {"true", "1", "yes", "y", "on"}
        falsy = {"false", "0", "no", "n", "off"}
        if normalized in truthy:
            return True
        if normalized in falsy:
            return False
    raise ValueError(f"Cannot coerce to bool: {value!r}")


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Cannot coerce to int: {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isfinite(value) and value.is_integer():
            return int(value)
        raise ValueError(f"Cannot coerce to int: {value!r}")
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError as exc:
            raise ValueError(f"Cannot coerce to int: {value!r}") from exc
    raise ValueError(f"Cannot coerce to int: {value!r}")


def _to_float(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError(f"Cannot coerce to float: {value!r}")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError as exc:
            raise ValueError(f"Cannot coerce to float: {value!r}") from exc
    raise ValueError(f"Cannot coerce to float: {value!r}")


def _to_rfc3339(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            dt = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"Cannot coerce to timestamp: {value!r}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return parsed.isoformat().replace("+00:00", "Z")
    raise ValueError(f"Cannot coerce to timestamp: {value!r}")


def _get_relative_value(data: Any, path: list[str], *, default: Any = _MISSING) -> Any:
    current = data
    for part in path:
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def _set_relative_value(data: dict[str, Any], path: list[str], value: Any) -> None:
    if not path:
        raise ValueError("Target path cannot be empty.")
    current = data
    for part in path[:-1]:
        next_value = current.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            current[part] = next_value
        current = next_value
    current[path[-1]] = value


def _stable_sorted(values: list[Any], key_path: list[str] | None, *, reverse: bool) -> list[Any]:
    decorated: list[tuple[bool, Any, Any]] = []
    for value in values:
        if key_path is None:
            key = _sort_token(value)
            missing = False
        else:
            extracted = _get_relative_value(value, key_path, default=_MISSING)
            missing = extracted is _MISSING
            key = _sort_token(extracted) if not missing else None
        decorated.append((missing, key, value))

    present = [item for item in decorated if not item[0]]
    missing = [item for item in decorated if item[0]]
    present_sorted = sorted(present, key=lambda item: item[1], reverse=reverse)
    return [item[2] for item in present_sorted] + [item[2] for item in missing]


def _sort_token(value: Any) -> tuple[int, Any]:
    if value is None:
        return (0, "")
    if isinstance(value, bool):
        return (1, 1 if value else 0)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (2, float(value))
    if isinstance(value, str):
        return (3, value)
    try:
        rendered = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except TypeError:
        rendered = str(value)
    return (4, rendered)


def _dedupe_first(values: list[Any], key_path: list[str] | None) -> list[Any]:
    seen: set[Any] = set()
    output: list[Any] = []
    for value in values:
        if key_path is None:
            key_value = value
            key_missing = False
        else:
            key_value = _get_relative_value(value, key_path, default=_MISSING)
            key_missing = key_value is _MISSING
        if key_missing:
            output.append(value)
            continue
        marker = _dedupe_marker(key_value)
        if marker in seen:
            continue
        seen.add(marker)
        output.append(value)
    return output


def _dedupe_marker(value: Any) -> Any:
    try:
        hash(value)
        return value
    except TypeError:
        try:
            return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        except TypeError:
            return str(value)


def _deep_merge(base: Any, incoming: Any) -> Any:
    if isinstance(base, dict) and isinstance(incoming, dict):
        merged = _clone(base)
        for key, value in incoming.items():
            if key in merged:
                merged[key] = _deep_merge(merged[key], value)
            else:
                merged[key] = _clone(value)
        return merged
    return _clone(incoming)


def _clone(value: Any) -> Any:
    return copy.deepcopy(value)


def _format_context_path(path: list[str]) -> str:
    if not path:
        return "context"
    return f"context.{'.'.join(path)}"
