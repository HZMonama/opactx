from __future__ import annotations

from pathlib import Path

import pytest

from opactx.core.events import CommandCompleted, FilePlanned
from opactx.core.init import init_events


@pytest.mark.integration
def test_init_default_plans_yaml_schema_not_json(tmp_path: Path) -> None:
    project = tmp_path / "project-default"
    events = list(
        init_events(
            project=project,
            force=False,
            minimal=False,
            with_examples=False,
            dry_run=True,
            name=None,
            no_policy=False,
            json_schema=False,
        )
    )

    planned = [event for event in events if isinstance(event, FilePlanned)]
    planned_paths = {str(Path(item.path).as_posix()) for item in planned if item.path}

    assert any(path.endswith("/schema/context.schema.yaml") for path in planned_paths)
    assert not any(path.endswith("/schema/context.schema.json") for path in planned_paths)

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True


@pytest.mark.integration
def test_init_default_writes_documented_starter_files(tmp_path: Path) -> None:
    project = tmp_path / "project-starter"
    events = list(
        init_events(
            project=project,
            force=False,
            minimal=False,
            with_examples=False,
            dry_run=False,
            name=None,
            no_policy=False,
            json_schema=False,
        )
    )

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True

    docs_link = "https://opactx.gitbook.io/opactx/"
    assert docs_link in (project / "opactx.yaml").read_text(encoding="utf-8")
    assert docs_link in (project / "README.md").read_text(encoding="utf-8")
    assert docs_link in (project / "context" / "standards.yaml").read_text(encoding="utf-8")
    assert docs_link in (project / "context" / "exceptions.yaml").read_text(encoding="utf-8")
    assert docs_link in (project / "policy" / "README.md").read_text(encoding="utf-8")

    schema_text = (project / "schema" / "context.schema.yaml").read_text(encoding="utf-8")
    assert docs_link in schema_text
    assert "fields: {}" in schema_text
    assert "allow_empty_object: true" in schema_text


@pytest.mark.integration
def test_init_with_examples_writes_populated_example_schema(tmp_path: Path) -> None:
    project = tmp_path / "project-example"
    events = list(
        init_events(
            project=project,
            force=False,
            minimal=False,
            with_examples=True,
            dry_run=False,
            name=None,
            no_policy=False,
            json_schema=False,
        )
    )

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True

    config_text = (project / "opactx.yaml").read_text(encoding="utf-8")
    assert "name: merge" in config_text
    assert "path: context.standards" in config_text
    assert "path: sources.inventory" in config_text

    schema_text = (project / "schema" / "context.schema.yaml").read_text(encoding="utf-8")
    assert "env:" in schema_text
    assert "actor:" in schema_text
    assert "request:" in schema_text
    assert "resources:" in schema_text
    assert "\n    sources:\n" not in schema_text

    standards_text = (project / "context" / "standards.yaml").read_text(encoding="utf-8")
    assert "actor:" in standards_text
    assert "request:" in standards_text


@pytest.mark.integration
def test_init_json_schema_mode_writes_json_only_and_updates_config(tmp_path: Path) -> None:
    project = tmp_path / "project-json"
    events = list(
        init_events(
            project=project,
            force=False,
            minimal=False,
            with_examples=False,
            dry_run=False,
            name=None,
            no_policy=False,
            json_schema=True,
        )
    )

    completed = events[-1]
    assert isinstance(completed, CommandCompleted)
    assert completed.ok is True

    config_text = (project / "opactx.yaml").read_text(encoding="utf-8")
    assert "schema: schema/context.schema.json" in config_text

    json_schema_text = (project / "schema" / "context.schema.json").read_text(encoding="utf-8")
    assert "https://opactx.gitbook.io/opactx/" in json_schema_text

    assert (project / "schema" / "context.schema.json").exists()
    assert not (project / "schema" / "context.schema.yaml").exists()
