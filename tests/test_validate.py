from __future__ import annotations

import json
from pathlib import Path

import pytest

from opactx.core.events import CommandCompleted, PluginMissing, StageCompleted, StageFailed, Warning
from opactx.core.validate import validate_events


@pytest.mark.integration
def test_validate_success_on_fixture_project(sample_project: Path) -> None:
    events = list(validate_events(sample_project))

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True
    assert completed.exit_code == 0


@pytest.mark.integration
def test_validate_strict_fails_unknown_source_plugin(sample_project: Path) -> None:
    config_path = sample_project / "opactx.yaml"
    config_text = config_path.read_text(encoding="utf-8")
    config_path.write_text(config_text.replace("type: file", "type: unknown"), encoding="utf-8")

    events = list(validate_events(sample_project, strict=True))

    assert any(isinstance(event, PluginMissing) for event in events)
    resolve_stage = next(
        event
        for event in events
        if isinstance(event, StageFailed) and event.stage_id == "resolve_plugins"
    )
    assert resolve_stage.error_code == "plugin_error"

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is False
    assert completed.exit_code == 2


@pytest.mark.integration
def test_validate_non_strict_warns_unknown_source_plugin(sample_project: Path) -> None:
    config_path = sample_project / "opactx.yaml"
    config_text = config_path.read_text(encoding="utf-8")
    config_path.write_text(config_text.replace("type: file", "type: unknown"), encoding="utf-8")

    events = list(validate_events(sample_project, strict=False))

    assert any(
        isinstance(event, Warning) and event.code == "plugin_missing" for event in events
    )
    resolve_stage = next(
        event
        for event in events
        if isinstance(event, StageCompleted) and event.stage_id == "resolve_plugins"
    )
    assert resolve_stage.status == "skipped"

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True
    assert completed.exit_code == 0


@pytest.mark.integration
def test_validate_non_strict_partial_when_schema_requires_sources(sample_project: Path) -> None:
    schema_path = sample_project / "schema" / "context.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["properties"]["sources"]["minProperties"] = 2
    schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")

    events = list(validate_events(sample_project, strict=False))

    assert any(
        isinstance(event, Warning) and event.code == "schema_partial" for event in events
    )
    schema_stage = next(
        event
        for event in events
        if isinstance(event, StageCompleted) and event.stage_id == "schema_check"
    )
    assert schema_stage.status == "partial"

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True
    assert completed.exit_code == 0


@pytest.mark.integration
def test_validate_strict_fails_unknown_builtin_transform_name(sample_project: Path) -> None:
    config_path = sample_project / "opactx.yaml"
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "name: canonicalize", "name: unknown_builtin"
        ),
        encoding="utf-8",
    )

    events = list(validate_events(sample_project, strict=True))

    resolve_stage = next(
        event
        for event in events
        if isinstance(event, StageFailed) and event.stage_id == "resolve_plugins"
    )
    assert resolve_stage.error_code == "plugin_error"
    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is False


@pytest.mark.integration
def test_validate_success_with_schema_dsl(sample_project: Path) -> None:
    config_path = sample_project / "opactx.yaml"
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "schema/context.schema.json",
            "schema/context.schema.yaml",
        ),
        encoding="utf-8",
    )
    (sample_project / "schema" / "context.schema.yaml").write_text(
        """
dsl: opactx.schema/v1
id: context
title: Policy Context
description: Canonical context contract used as data.context
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

    events = list(validate_events(sample_project, strict=True))

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True
    assert completed.exit_code == 0


@pytest.mark.integration
def test_validate_fails_invalid_schema_dsl_version(sample_project: Path) -> None:
    config_path = sample_project / "opactx.yaml"
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "schema/context.schema.json",
            "schema/context.schema.yaml",
        ),
        encoding="utf-8",
    )
    (sample_project / "schema" / "context.schema.yaml").write_text(
        """
dsl: opactx.schema/v2
id: context
title: Policy Context
description: Canonical context contract used as data.context
root: context
schema:
  type: object
  allow_empty_object: true
""".strip()
        + "\n",
        encoding="utf-8",
    )

    events = list(validate_events(sample_project, strict=True))

    stage_failed = next(
        event
        for event in events
        if isinstance(event, StageFailed) and event.stage_id == "load_schema"
    )
    assert stage_failed.error_code == "schema_error"
    assert "meta-schema validation failed" in stage_failed.message
    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is False


@pytest.mark.integration
def test_validate_non_strict_partial_for_transform_assembled_context(sample_project: Path) -> None:
    (sample_project / "opactx.yaml").write_text(
        """
version: v1

schema: schema/context.schema.json
context_dir: context

sources:
  - name: inventory
    type: file
    with:
      path: fixtures/inventory.json

transforms:
  - name: canonicalize
    type: builtin
    with: {}
  - name: merge
    type: builtin
    with:
      target: context
      from:
        - path: context.standards
        - path: context.exceptions
        - path: sources.inventory

output:
  dir: dist/bundle
  include_policy: false
  tarball: false
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (sample_project / "context" / "standards.yaml").write_text(
        """
env: dev
actor:
  id: user-123
  role: admin
request:
  action: deploy
  resource:
    type: service
    id: payments-api
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (sample_project / "context" / "exceptions.yaml").write_text(
        """
exceptions:
  - id: EX-1
    control: deploy_window
    owner: team-platform
    expires_at: "2026-12-31T00:00:00Z"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (sample_project / "schema" / "context.schema.json").write_text(
        """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["env", "actor", "request", "exceptions", "resources"],
  "properties": {
    "env": { "type": "string" },
    "actor": { "type": "object" },
    "request": { "type": "object" },
    "exceptions": { "type": "array" },
    "resources": { "type": "array" }
  },
  "additionalProperties": false
}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    events = list(validate_events(sample_project, strict=False))

    assert any(isinstance(event, Warning) and event.code == "schema_partial" for event in events)
    schema_stage = next(
        event
        for event in events
        if isinstance(event, StageCompleted) and event.stage_id == "schema_check"
    )
    assert schema_stage.status == "partial"
    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True


@pytest.mark.integration
def test_validate_strict_fails_for_transform_assembled_context_needing_sources(sample_project: Path) -> None:
    (sample_project / "opactx.yaml").write_text(
        """
version: v1

schema: schema/context.schema.json
context_dir: context

sources:
  - name: inventory
    type: file
    with:
      path: fixtures/inventory.json

transforms:
  - name: canonicalize
    type: builtin
    with: {}
  - name: merge
    type: builtin
    with:
      target: context
      from:
        - path: context.standards
        - path: context.exceptions
        - path: sources.inventory

output:
  dir: dist/bundle
  include_policy: false
  tarball: false
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (sample_project / "context" / "standards.yaml").write_text(
        """
env: dev
actor:
  id: user-123
  role: admin
request:
  action: deploy
  resource:
    type: service
    id: payments-api
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (sample_project / "context" / "exceptions.yaml").write_text(
        """
exceptions:
  - id: EX-1
    control: deploy_window
    owner: team-platform
    expires_at: "2026-12-31T00:00:00Z"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (sample_project / "schema" / "context.schema.json").write_text(
        """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["env", "actor", "request", "exceptions", "resources"],
  "properties": {
    "env": { "type": "string" },
    "actor": { "type": "object" },
    "request": { "type": "object" },
    "exceptions": { "type": "array" },
    "resources": { "type": "array" }
  },
  "additionalProperties": false
}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    events = list(validate_events(sample_project, strict=True))

    schema_stage = next(
        event
        for event in events
        if isinstance(event, StageFailed) and event.stage_id == "schema_check"
    )
    assert "sources are fetched" in schema_stage.message
    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is False
