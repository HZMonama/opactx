from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import ValidationError
from ruamel.yaml import YAML

from .model import Config


class ConfigError(RuntimeError):
    pass


_yaml = YAML(typ="safe")


def load_config(project_dir: Path, config_path: Path | None = None) -> Config:
    config_path = config_path or Path("opactx.yaml")
    if not config_path.is_absolute():
        config_path = project_dir / config_path
    if not config_path.exists():
        raise ConfigError(f"Missing config: {config_path}")
    data = _load_yaml(config_path)
    if not isinstance(data, dict):
        raise ConfigError("Config must be a YAML mapping at the top level.")
    try:
        return Config.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(str(exc)) from exc
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc


def load_yaml_mapping(path: Path, *, required: bool) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise ConfigError(f"Missing required file: {path}")
        return {}
    data = _load_yaml(path)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must be a YAML mapping at the top level.")
    return data


def _load_yaml(path: Path) -> Any:
    try:
        return _yaml.load(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise ConfigError(f"Failed to parse YAML: {path}") from exc
