from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from opactx.core.build import build_events
from opactx.core.events import (
    BundleWritten,
    CommandCompleted,
    SourceFetchFailed,
    StageCompleted,
    StageFailed,
)


@pytest.mark.integration
def test_build_dry_run_skips_bundle_write(sample_project: Path) -> None:
    events = list(build_events(project_dir=sample_project, dry_run=True))

    write_stage = next(
        event
        for event in events
        if isinstance(event, StageCompleted) and event.stage_id == "write_bundle"
    )
    assert write_stage.status == "skipped"

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True
    assert completed.exit_code == 0
    assert not (sample_project / "dist" / "bundle" / "data.json").exists()


@pytest.mark.integration
def test_build_writes_bundle_with_manifest_revision(sample_project: Path) -> None:
    output_dir = sample_project / "out" / "bundle"
    events = list(build_events(project_dir=sample_project, output_dir=output_dir))

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True

    data_bytes = (output_dir / "data.json").read_bytes()
    manifest = json.loads((output_dir / ".manifest").read_text(encoding="utf-8"))
    expected_revision = hashlib.sha256(data_bytes).hexdigest()

    assert manifest["revision"] == expected_revision

    bundle_written = next(event for event in events if isinstance(event, BundleWritten))
    assert bundle_written.revision == expected_revision


@pytest.mark.integration
def test_build_fails_when_source_fetch_fails(sample_project: Path) -> None:
    config_path = sample_project / "opactx.yaml"
    config_text = config_path.read_text(encoding="utf-8")
    config_path.write_text(
        config_text.replace("fixtures/inventory.json", "fixtures/does-not-exist.json"),
        encoding="utf-8",
    )

    events = list(build_events(project_dir=sample_project))

    assert any(isinstance(event, SourceFetchFailed) for event in events)
    stage_failed = next(
        event
        for event in events
        if isinstance(event, StageFailed) and event.stage_id == "fetch_sources"
    )
    assert stage_failed.error_code == "source_error"

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is False
    assert completed.exit_code == 2


@pytest.mark.integration
def test_build_applies_builtin_transform_pipeline(sample_project: Path) -> None:
    config_path = sample_project / "opactx.yaml"
    config_path.write_text(
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
  - name: mount
    type: builtin
    with:
      source_id: inventory
      target: context.inventory
  - name: defaults
    type: builtin
    with:
      values:
        context.env: dev

output:
  dir: dist/bundle
  include_policy: false
  tarball: false
""".strip()
        + "\n",
        encoding="utf-8",
    )

    schema_path = sample_project / "schema" / "context.schema.json"
    schema_path.write_text(
        """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["standards", "exceptions", "sources", "inventory", "env"],
  "properties": {
    "standards": { "type": "object" },
    "exceptions": { "type": "object" },
    "sources": { "type": "object" },
    "inventory": { "type": "object" },
    "env": { "type": "string" }
  },
  "additionalProperties": false
}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    output_dir = sample_project / "out" / "bundle"
    events = list(build_events(project_dir=sample_project, output_dir=output_dir))

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True
    data = json.loads((output_dir / "data.json").read_text(encoding="utf-8"))
    assert data["context"]["env"] == "dev"
    assert "resources" in data["context"]["inventory"]


@pytest.mark.integration
def test_build_with_schema_dsl_emits_compiled_artifact(sample_project: Path) -> None:
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

    output_dir = sample_project / "out" / "bundle"
    events = list(build_events(project_dir=sample_project, output_dir=output_dir))

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True
    assert (sample_project / "build" / "schema" / "context.schema.json").exists()
