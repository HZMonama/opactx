from __future__ import annotations

import json
from pathlib import Path

import pytest

from opactx.schema.dsl import (
    SchemaDslError,
    compile_context_schema,
    load_compiled_schema,
    validate_schema_dsl_document,
)


def test_compile_context_schema_emits_json_schema() -> None:
    document = {
        "dsl": "opactx.schema/v1",
        "id": "context",
        "title": "Policy Context",
        "description": "Canonical context contract",
        "root": "context",
        "strict": True,
        "definitions": {
            "Team": {
                "type": "object",
                "fields": {
                    "id": {"type": "string", "required": True},
                    "name": {"type": "string", "required": True},
                },
            }
        },
        "schema": {
            "type": "object",
            "fields": {
                "env": {"type": "string", "required": True, "enum": ["dev", "prod"]},
                "teams": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/Team"},
                    "unique_by": "id",
                },
            },
        },
    }

    compiled = compile_context_schema(document)

    assert compiled["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert compiled["type"] == "object"
    assert compiled["additionalProperties"] is False
    assert compiled["required"] == ["env"]
    assert compiled["properties"]["teams"]["items"]["$ref"] == "#/$defs/Team"
    assert compiled["properties"]["teams"]["x-opactx-uniqueBy"] == "id"
    assert compiled["$defs"]["Team"]["additionalProperties"] is False


def test_compile_context_schema_rejects_invalid_default_type() -> None:
    document = {
        "dsl": "opactx.schema/v1",
        "id": "context",
        "title": "Policy Context",
        "description": "Canonical context contract",
        "root": "context",
        "schema": {
            "type": "object",
            "fields": {
                "retries": {"type": "integer", "default": "1"},
            },
        },
    }

    with pytest.raises(SchemaDslError, match="must be an integer"):
        compile_context_schema(document)


def test_compile_context_schema_rejects_ref_cycles() -> None:
    document = {
        "dsl": "opactx.schema/v1",
        "id": "context",
        "title": "Policy Context",
        "description": "Canonical context contract",
        "root": "context",
        "definitions": {
            "A": {"type": "object", "fields": {"b": {"$ref": "#/definitions/B"}}},
            "B": {"type": "object", "fields": {"a": {"$ref": "#/definitions/A"}}},
        },
        "schema": {
            "type": "object",
            "allow_empty_object": True,
        },
    }

    with pytest.raises(SchemaDslError, match="Reference cycle detected"):
        compile_context_schema(document)


def test_load_compiled_schema_from_yaml_emits_artifact(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    schema_dir = project_dir / "schema"
    schema_dir.mkdir(parents=True)

    (schema_dir / "context.schema.yaml").write_text(
        """
dsl: opactx.schema/v1
id: context
title: Policy Context
description: Canonical context contract
root: context
strict: true
schema:
  type: object
  fields:
    standards:
      type: object
      required: true
      strict: false
      allow_empty_object: true
    exceptions:
      type: object
      required: true
      strict: false
      allow_empty_object: true
    sources:
      type: object
      required: true
      strict: false
      allow_empty_object: true
""".strip()
        + "\n",
        encoding="utf-8",
    )

    compiled = load_compiled_schema(
        project_dir=project_dir,
        schema_path=Path("schema/context.schema.yaml"),
        emit_compiled_artifact=True,
    )

    assert compiled["type"] == "object"
    artifact = project_dir / "build" / "schema" / "context.schema.json"
    assert artifact.exists()
    emitted = json.loads(artifact.read_text(encoding="utf-8"))
    assert emitted["title"] == "Policy Context"


def test_validate_schema_dsl_document_rejects_invalid_shape() -> None:
    document = {
        "dsl": "opactx.schema/v1",
        "id": "context",
        "title": "Policy Context",
        "description": "Canonical context contract",
        "root": "context",
        "schema": {
            "type": "object",
            "fields": {
                "env": {
                    "required": True,
                }
            },
        },
    }

    with pytest.raises(SchemaDslError, match="meta-schema validation failed"):
        validate_schema_dsl_document(document)
