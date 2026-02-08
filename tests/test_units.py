from __future__ import annotations

import sys
from pathlib import Path

import pytest

from opactx.config.load import ConfigError, load_yaml_mapping
from opactx.core.events import CommandStarted
from opactx.sources.exec import ExecSource


def test_load_yaml_mapping_optional_missing_returns_empty(tmp_path: Path) -> None:
    assert load_yaml_mapping(tmp_path / "missing.yaml", required=False) == {}


def test_load_yaml_mapping_required_missing_raises(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    with pytest.raises(ConfigError, match="Missing required file"):
        load_yaml_mapping(missing, required=True)


def test_exec_source_parses_json_stdout(tmp_path: Path) -> None:
    source = ExecSource(
        tmp_path,
        cmd=[sys.executable, "-c", 'import json; print(json.dumps({"ok": True}))'],
    )
    assert source.fetch() == {"ok": True}


def test_exec_source_rejects_non_list_cmd(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="list of strings"):
        ExecSource(tmp_path, cmd="python -V")  # type: ignore[arg-type]


def test_event_to_dict_serializes_paths() -> None:
    event = CommandStarted(
        command="validate",
        project_dir=Path("project"),
        config_path=Path("project") / "opactx.yaml",
    )
    payload = event.to_dict()

    assert payload["project_dir"] == "project"
    assert payload["config_path"] == str(Path("project") / "opactx.yaml")
