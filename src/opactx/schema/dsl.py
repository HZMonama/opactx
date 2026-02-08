from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from ruamel.yaml import YAML

from opactx.schema.meta_schema import CONTEXT_SCHEMA_DSL_META_SCHEMA

_DRAFT_2020_12 = "https://json-schema.org/draft/2020-12/schema"
_DSL_VERSION = "opactx.schema/v1"

_yaml = YAML(typ="safe")

_BASE_TYPES = {"object", "array", "string", "number", "integer", "boolean", "null"}
_STRING_FORMATS = {"date-time", "email", "uri", "uuid"}

_TOP_LEVEL_KEYS = {
    "dsl",
    "id",
    "title",
    "description",
    "root",
    "strict",
    "schema",
    "definitions",
}
_TOP_LEVEL_REQUIRED = {"dsl", "id", "title", "description", "root", "schema"}

_COMMON_KEYS = {"type", "description", "nullable", "default", "examples", "deprecated", "tags"}


class SchemaLoadError(RuntimeError):
    pass


class SchemaDslError(SchemaLoadError):
    pass


def load_compiled_schema(
    *,
    project_dir: Path,
    schema_path: Path,
    emit_compiled_artifact: bool = False,
) -> dict[str, Any]:
    resolved = schema_path
    if not resolved.is_absolute():
        resolved = project_dir / resolved
    if not resolved.exists():
        raise SchemaLoadError(f"Schema not found: {resolved}")

    suffix = resolved.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        document = _load_yaml_mapping(resolved)
        validate_schema_dsl_document(document)
        compiled = compile_context_schema(document)
        if emit_compiled_artifact:
            artifact = compiled_schema_artifact_path(project_dir, resolved)
            artifact.parent.mkdir(parents=True, exist_ok=True)
            artifact.write_text(_stable_json_text(compiled), encoding="utf-8")
    else:
        compiled = _load_json_mapping(resolved)

    if not isinstance(compiled, dict):
        raise SchemaLoadError("Schema must be a JSON object.")
    try:
        Draft202012Validator.check_schema(compiled)
    except SchemaError as exc:
        raise SchemaLoadError(f"Schema is not valid: {exc.message}") from exc
    return compiled


def compiled_schema_artifact_path(project_dir: Path, dsl_schema_path: Path) -> Path:
    filename = dsl_schema_path.with_suffix(".json").name
    return project_dir / "build" / "schema" / filename


def compile_context_schema(document: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise SchemaDslError("Schema DSL must be a mapping at the top level.")
    _reject_unknown_keys(document, _TOP_LEVEL_KEYS, "root")
    _require_keys(document, _TOP_LEVEL_REQUIRED, "root")

    dsl_version = document.get("dsl")
    if dsl_version != _DSL_VERSION:
        raise SchemaDslError(
            f"Unsupported schema DSL version: {dsl_version!r}. Expected {_DSL_VERSION!r}."
        )
    schema_id = _require_non_empty_string(document, "id", "root")
    title = _require_non_empty_string(document, "title", "root")
    description = _require_non_empty_string(document, "description", "root")
    root_name = _require_non_empty_string(document, "root", "root")
    strict_default = document.get("strict", True)
    if not isinstance(strict_default, bool):
        raise SchemaDslError("root.strict must be a boolean when provided.")

    root_schema = document.get("schema")
    if not isinstance(root_schema, dict):
        raise SchemaDslError("root.schema must be a mapping.")
    root_type = root_schema.get("type")
    if root_type != "object":
        raise SchemaDslError("root.schema.type must be 'object' in v0.1.")

    definitions = document.get("definitions", {})
    if not isinstance(definitions, dict):
        raise SchemaDslError("root.definitions must be a mapping when provided.")
    for def_name in definitions:
        if not isinstance(def_name, str) or not def_name:
            raise SchemaDslError("Definition names must be non-empty strings.")
        if not isinstance(definitions[def_name], dict):
            raise SchemaDslError(f"Definition '{def_name}' must be a mapping.")

    _validate_references(root_schema, definitions)

    compiled: dict[str, Any] = {
        "$schema": _DRAFT_2020_12,
        "title": title,
        "description": description,
        "x-opactx-id": schema_id,
        "x-opactx-root": root_name,
    }
    compiled_root = _compile_node(
        root_schema,
        path="schema",
        inherited_strict=strict_default,
        field_context=False,
    )
    compiled.update(compiled_root)

    if definitions:
        defs: dict[str, Any] = {}
        for name, node in definitions.items():
            defs[name] = _compile_node(
                node,
                path=f"definitions.{name}",
                inherited_strict=strict_default,
                field_context=False,
            )
        compiled["$defs"] = defs

    return compiled


def validate_schema_dsl_document(document: dict[str, Any]) -> None:
    if not isinstance(document, dict):
        raise SchemaDslError("Schema DSL must be a mapping at the top level.")

    try:
        Draft202012Validator.check_schema(CONTEXT_SCHEMA_DSL_META_SCHEMA)
    except SchemaError as exc:
        raise SchemaDslError(f"Internal DSL meta-schema is invalid: {exc.message}") from exc

    validator = Draft202012Validator(CONTEXT_SCHEMA_DSL_META_SCHEMA)
    errors = sorted(validator.iter_errors(document), key=lambda err: list(err.absolute_path))
    if not errors:
        return

    first = errors[0]
    path = _format_error_path(first.absolute_path)
    raise SchemaDslError(f"Schema DSL meta-schema validation failed at {path}: {first.message}")


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        parsed = _yaml.load(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise SchemaLoadError(f"Failed to parse schema DSL YAML: {path}") from exc
    if not isinstance(parsed, dict):
        raise SchemaLoadError("Schema DSL must be a mapping at the top level.")
    return parsed


def _load_json_mapping(path: Path) -> dict[str, Any]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SchemaLoadError(f"Invalid JSON schema: {path}") from exc
    if not isinstance(parsed, dict):
        raise SchemaLoadError("Schema must be a JSON object.")
    return parsed


def _stable_json_text(data: dict[str, Any]) -> str:
    payload = json.dumps(data, sort_keys=True, indent=2, ensure_ascii=False)
    return f"{payload}\n"


def _validate_references(schema_node: dict[str, Any], definitions: dict[str, Any]) -> None:
    definition_names = set(definitions.keys())

    root_refs = _collect_refs(schema_node, path="schema")
    for ref_name in root_refs:
        if ref_name not in definition_names:
            raise SchemaDslError(f"Reference not found: {ref_name}")

    graph: dict[str, set[str]] = {}
    for def_name, node in definitions.items():
        refs = _collect_refs(node, path=f"definitions.{def_name}")
        graph[def_name] = refs
        for ref_name in refs:
            if ref_name not in definition_names:
                raise SchemaDslError(f"Reference not found: {ref_name}")

    visit_state: dict[str, int] = {}
    stack: list[str] = []

    def dfs(name: str) -> None:
        state = visit_state.get(name, 0)
        if state == 1:
            cycle_start = stack.index(name)
            cycle = stack[cycle_start:] + [name]
            raise SchemaDslError("Reference cycle detected: " + " -> ".join(cycle))
        if state == 2:
            return
        visit_state[name] = 1
        stack.append(name)
        for target in graph.get(name, set()):
            dfs(target)
        stack.pop()
        visit_state[name] = 2

    for def_name in sorted(definition_names):
        dfs(def_name)


def _collect_refs(node: Any, *, path: str) -> set[str]:
    if not isinstance(node, dict):
        raise SchemaDslError(f"{path} must be a mapping.")

    refs: set[str] = set()
    if "$ref" in node:
        ref_name = _definition_ref_name(node["$ref"], path=path)
        refs.add(ref_name)
        return refs

    node_type = node.get("type")
    if node_type == "object":
        fields = node.get("fields", {})
        if fields is None:
            fields = {}
        if not isinstance(fields, dict):
            raise SchemaDslError(f"{path}.fields must be a mapping.")
        for field_name, field_node in fields.items():
            refs |= _collect_refs(field_node, path=f"{path}.fields.{field_name}")
    elif node_type == "array":
        items = node.get("items")
        if items is None:
            return refs
        refs |= _collect_refs(items, path=f"{path}.items")
    return refs


def _compile_node(
    node: dict[str, Any],
    *,
    path: str,
    inherited_strict: bool,
    field_context: bool,
) -> dict[str, Any]:
    if not isinstance(node, dict):
        raise SchemaDslError(f"{path} must be a mapping.")

    if "$ref" in node:
        return _compile_ref_node(
            node,
            path=path,
            field_context=field_context,
        )

    if "type" not in node:
        raise SchemaDslError(f"{path} must define either 'type' or '$ref'.")
    node_type = node.get("type")
    if not isinstance(node_type, str) or node_type not in _BASE_TYPES:
        raise SchemaDslError(f"{path}.type is not supported: {node_type!r}")

    allowed_keys = set(_COMMON_KEYS)
    if field_context:
        allowed_keys.add("required")
    if node_type == "object":
        allowed_keys |= {"fields", "strict", "allow_empty_object"}
    elif node_type == "array":
        allowed_keys |= {"items", "min_items", "max_items", "unique_by"}
    elif node_type == "string":
        allowed_keys |= {"min_len", "max_len", "pattern", "enum", "format"}
    elif node_type in {"number", "integer"}:
        allowed_keys |= {"min", "max", "enum"}
    elif node_type in {"boolean", "null"}:
        allowed_keys |= {"enum"}

    _reject_unknown_keys(node, allowed_keys, path)

    compiled: dict[str, Any] = {"type": node_type}
    _apply_common_keywords(
        compiled=compiled,
        node=node,
        node_type=node_type,
        path=path,
    )

    if node_type == "object":
        compiled = _compile_object_node(
            compiled=compiled,
            node=node,
            path=path,
            inherited_strict=inherited_strict,
        )
    elif node_type == "array":
        compiled = _compile_array_node(
            compiled=compiled,
            node=node,
            path=path,
            inherited_strict=inherited_strict,
        )
    elif node_type == "string":
        compiled = _compile_string_node(compiled=compiled, node=node, path=path)
    elif node_type in {"number", "integer"}:
        compiled = _compile_number_node(compiled=compiled, node=node, path=path)
    elif node_type in {"boolean", "null"}:
        compiled = _compile_scalar_enum(compiled=compiled, node=node, node_type=node_type, path=path)

    nullable = node.get("nullable", False)
    if nullable is True and node_type != "null":
        compiled["type"] = [node_type, "null"]
    return compiled


def _compile_ref_node(
    node: dict[str, Any],
    *,
    path: str,
    field_context: bool,
) -> dict[str, Any]:
    allowed = {"$ref", "description", "deprecated"}
    if field_context:
        allowed.add("required")
    _reject_unknown_keys(node, allowed, path)
    ref_name = _definition_ref_name(node.get("$ref"), path=path)
    compiled = {"$ref": f"#/$defs/{ref_name}"}
    if "description" in node:
        description = node["description"]
        if not isinstance(description, str):
            raise SchemaDslError(f"{path}.description must be a string.")
        compiled["description"] = description
    if "deprecated" in node:
        deprecated = node["deprecated"]
        if not isinstance(deprecated, bool):
            raise SchemaDslError(f"{path}.deprecated must be a boolean.")
        compiled["deprecated"] = deprecated
    return compiled


def _compile_object_node(
    *,
    compiled: dict[str, Any],
    node: dict[str, Any],
    path: str,
    inherited_strict: bool,
) -> dict[str, Any]:
    allow_empty = node.get("allow_empty_object", False)
    if not isinstance(allow_empty, bool):
        raise SchemaDslError(f"{path}.allow_empty_object must be a boolean.")
    strict = node.get("strict", inherited_strict)
    if not isinstance(strict, bool):
        raise SchemaDslError(f"{path}.strict must be a boolean when provided.")

    fields = node.get("fields")
    if fields is None:
        if not allow_empty:
            raise SchemaDslError(f"{path}.fields is required unless allow_empty_object is true.")
        fields = {}
    if not isinstance(fields, dict):
        raise SchemaDslError(f"{path}.fields must be a mapping.")
    if not fields and not allow_empty:
        raise SchemaDslError(f"{path}.fields must not be empty unless allow_empty_object is true.")

    properties: dict[str, Any] = {}
    required_fields: list[str] = []
    for field_name, field_node in fields.items():
        if not isinstance(field_name, str) or not field_name:
            raise SchemaDslError(f"{path}.fields contains an invalid field name.")
        if not isinstance(field_node, dict):
            raise SchemaDslError(f"{path}.fields.{field_name} must be a mapping.")
        field_required = field_node.get("required", False)
        if not isinstance(field_required, bool):
            raise SchemaDslError(f"{path}.fields.{field_name}.required must be a boolean.")
        properties[field_name] = _compile_node(
            field_node,
            path=f"{path}.fields.{field_name}",
            inherited_strict=strict,
            field_context=True,
        )
        if field_required:
            required_fields.append(field_name)

    compiled["properties"] = properties
    if required_fields:
        compiled["required"] = required_fields
    if strict:
        compiled["additionalProperties"] = False
    else:
        compiled["additionalProperties"] = True
    return compiled


def _compile_array_node(
    *,
    compiled: dict[str, Any],
    node: dict[str, Any],
    path: str,
    inherited_strict: bool,
) -> dict[str, Any]:
    items = node.get("items")
    if items is None:
        raise SchemaDslError(f"{path}.items is required for arrays.")
    if not isinstance(items, dict):
        raise SchemaDslError(f"{path}.items must be a mapping.")
    compiled["items"] = _compile_node(
        items,
        path=f"{path}.items",
        inherited_strict=inherited_strict,
        field_context=False,
    )

    if "min_items" in node:
        min_items = node["min_items"]
        if not isinstance(min_items, int) or min_items < 0:
            raise SchemaDslError(f"{path}.min_items must be a non-negative integer.")
        compiled["minItems"] = min_items
    if "max_items" in node:
        max_items = node["max_items"]
        if not isinstance(max_items, int) or max_items < 0:
            raise SchemaDslError(f"{path}.max_items must be a non-negative integer.")
        compiled["maxItems"] = max_items
    if "min_items" in node and "max_items" in node:
        if node["min_items"] > node["max_items"]:
            raise SchemaDslError(f"{path}.min_items must be less than or equal to max_items.")

    unique_by = node.get("unique_by")
    if unique_by is not None:
        if not isinstance(unique_by, str) or not unique_by.strip():
            raise SchemaDslError(f"{path}.unique_by must be a non-empty string.")
        compiled["x-opactx-uniqueBy"] = unique_by.strip()
    return compiled


def _compile_string_node(*, compiled: dict[str, Any], node: dict[str, Any], path: str) -> dict[str, Any]:
    if "min_len" in node:
        min_len = node["min_len"]
        if not isinstance(min_len, int) or min_len < 0:
            raise SchemaDslError(f"{path}.min_len must be a non-negative integer.")
        compiled["minLength"] = min_len
    if "max_len" in node:
        max_len = node["max_len"]
        if not isinstance(max_len, int) or max_len < 0:
            raise SchemaDslError(f"{path}.max_len must be a non-negative integer.")
        compiled["maxLength"] = max_len
    if "min_len" in node and "max_len" in node:
        if node["min_len"] > node["max_len"]:
            raise SchemaDslError(f"{path}.min_len must be less than or equal to max_len.")

    if "pattern" in node:
        pattern = node["pattern"]
        if not isinstance(pattern, str):
            raise SchemaDslError(f"{path}.pattern must be a string.")
        compiled["pattern"] = pattern

    if "format" in node:
        fmt = node["format"]
        if not isinstance(fmt, str) or fmt not in _STRING_FORMATS:
            allowed = ", ".join(sorted(_STRING_FORMATS))
            raise SchemaDslError(f"{path}.format must be one of: {allowed}.")
        compiled["format"] = fmt

    if "enum" in node:
        enum_values = node["enum"]
        if not isinstance(enum_values, list):
            raise SchemaDslError(f"{path}.enum must be a list.")
        nullable = bool(node.get("nullable", False))
        for index, value in enumerate(enum_values):
            _assert_type(value, "string", nullable=nullable, path=f"{path}.enum[{index}]")
        compiled["enum"] = enum_values
    return compiled


def _compile_number_node(
    *,
    compiled: dict[str, Any],
    node: dict[str, Any],
    path: str,
) -> dict[str, Any]:
    if "min" in node:
        minimum = node["min"]
        if not _is_number(minimum):
            raise SchemaDslError(f"{path}.min must be numeric.")
        compiled["minimum"] = minimum
    if "max" in node:
        maximum = node["max"]
        if not _is_number(maximum):
            raise SchemaDslError(f"{path}.max must be numeric.")
        compiled["maximum"] = maximum
    if "min" in node and "max" in node and node["min"] > node["max"]:
        raise SchemaDslError(f"{path}.min must be less than or equal to max.")

    if "enum" in node:
        enum_values = node["enum"]
        if not isinstance(enum_values, list):
            raise SchemaDslError(f"{path}.enum must be a list.")
        nullable = bool(node.get("nullable", False))
        for index, value in enumerate(enum_values):
            _assert_type(
                value,
                str(compiled["type"]),
                nullable=nullable,
                path=f"{path}.enum[{index}]",
            )
        compiled["enum"] = enum_values
    return compiled


def _compile_scalar_enum(
    *,
    compiled: dict[str, Any],
    node: dict[str, Any],
    node_type: str,
    path: str,
) -> dict[str, Any]:
    if "enum" not in node:
        return compiled
    enum_values = node["enum"]
    if not isinstance(enum_values, list):
        raise SchemaDslError(f"{path}.enum must be a list.")
    nullable = bool(node.get("nullable", False))
    for index, value in enumerate(enum_values):
        _assert_type(value, node_type, nullable=nullable, path=f"{path}.enum[{index}]")
    compiled["enum"] = enum_values
    return compiled


def _apply_common_keywords(
    *,
    compiled: dict[str, Any],
    node: dict[str, Any],
    node_type: str,
    path: str,
) -> None:
    if "description" in node:
        description = node["description"]
        if not isinstance(description, str):
            raise SchemaDslError(f"{path}.description must be a string.")
        compiled["description"] = description

    if "deprecated" in node:
        deprecated = node["deprecated"]
        if not isinstance(deprecated, bool):
            raise SchemaDslError(f"{path}.deprecated must be a boolean.")
        compiled["deprecated"] = deprecated

    if "tags" in node:
        tags = node["tags"]
        if not isinstance(tags, list) or not all(isinstance(item, str) for item in tags):
            raise SchemaDslError(f"{path}.tags must be a list of strings.")
        compiled["x-opactx-tags"] = tags

    nullable = node.get("nullable", False)
    if not isinstance(nullable, bool):
        raise SchemaDslError(f"{path}.nullable must be a boolean.")

    if "default" in node:
        value = node["default"]
        _assert_type(value, node_type, nullable=nullable, path=f"{path}.default")
        compiled["default"] = value

    if "examples" in node:
        examples = node["examples"]
        if not isinstance(examples, list):
            raise SchemaDslError(f"{path}.examples must be a list.")
        for index, value in enumerate(examples):
            _assert_type(value, node_type, nullable=nullable, path=f"{path}.examples[{index}]")
        compiled["examples"] = examples


def _assert_type(value: Any, node_type: str, *, nullable: bool, path: str) -> None:
    if value is None:
        if nullable or node_type == "null":
            return
        raise SchemaDslError(f"{path} must not be null.")

    if node_type == "object":
        if not isinstance(value, dict):
            raise SchemaDslError(f"{path} must be an object.")
        return
    if node_type == "array":
        if not isinstance(value, list):
            raise SchemaDslError(f"{path} must be an array.")
        return
    if node_type == "string":
        if not isinstance(value, str):
            raise SchemaDslError(f"{path} must be a string.")
        return
    if node_type == "number":
        if not _is_number(value):
            raise SchemaDslError(f"{path} must be a number.")
        return
    if node_type == "integer":
        if not _is_integer(value):
            raise SchemaDslError(f"{path} must be an integer.")
        return
    if node_type == "boolean":
        if not isinstance(value, bool):
            raise SchemaDslError(f"{path} must be a boolean.")
        return
    if node_type == "null":
        raise SchemaDslError(f"{path} must be null.")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _reject_unknown_keys(mapping: dict[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(mapping.keys()) - allowed)
    if not unknown:
        return
    joined = ", ".join(unknown)
    raise SchemaDslError(f"{path} has unknown keys: {joined}")


def _require_keys(mapping: dict[str, Any], required: set[str], path: str) -> None:
    missing = sorted(key for key in required if key not in mapping)
    if not missing:
        return
    joined = ", ".join(missing)
    raise SchemaDslError(f"{path} is missing required keys: {joined}")


def _require_non_empty_string(mapping: dict[str, Any], key: str, path: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SchemaDslError(f"{path}.{key} must be a non-empty string.")
    return value.strip()


def _definition_ref_name(ref: Any, *, path: str) -> str:
    if not isinstance(ref, str):
        raise SchemaDslError(f"{path}.$ref must be a string.")
    if ref.startswith("#/definitions/"):
        name = ref[len("#/definitions/") :]
    elif ref.startswith("#/$defs/"):
        name = ref[len("#/$defs/") :]
    else:
        raise SchemaDslError(
            f"{path}.$ref must use #/definitions/<Name> (or #/$defs/<Name>): {ref}"
        )
    if "/" in name or not name:
        raise SchemaDslError(f"{path}.$ref target is invalid: {ref}")
    return name


def _format_error_path(path_parts: Any) -> str:
    parts = [str(part) for part in path_parts]
    if not parts:
        return "root"
    return "root." + ".".join(parts)
