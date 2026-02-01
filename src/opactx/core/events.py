from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OpactxEvent:
    ts: float = field(default_factory=time.perf_counter)
    level: str = "INFO"
    command: str = ""
    type: str = ""

    def to_dict(self) -> dict[str, Any]:
        return _serialize(asdict(self))


@dataclass(frozen=True)
class CommandStarted(OpactxEvent):
    type: str = "CommandStarted"
    project_dir: Path | None = None
    config_path: Path | None = None
    options: dict[str, Any] | None = None


@dataclass(frozen=True)
class CommandCompleted(OpactxEvent):
    type: str = "CommandCompleted"
    ok: bool = True
    exit_code: int = 0


@dataclass(frozen=True)
class StageStarted(OpactxEvent):
    type: str = "StageStarted"
    stage_id: str = ""
    label: str = ""


@dataclass(frozen=True)
class StageProgress(OpactxEvent):
    type: str = "StageProgress"
    stage_id: str = ""
    current: int = 0
    total: int = 0
    note: str | None = None


@dataclass(frozen=True)
class StageCompleted(OpactxEvent):
    type: str = "StageCompleted"
    stage_id: str = ""
    duration_ms: float = 0.0
    status: str = "success"


@dataclass(frozen=True)
class StageFailed(OpactxEvent):
    type: str = "StageFailed"
    stage_id: str = ""
    duration_ms: float = 0.0
    error_code: str = ""
    message: str = ""
    hint: str | None = None


@dataclass(frozen=True)
class FilePlanned(OpactxEvent):
    type: str = "FilePlanned"
    op: str = ""
    path: Path | None = None


@dataclass(frozen=True)
class FileWritten(OpactxEvent):
    type: str = "FileWritten"
    path: Path | None = None
    bytes: int = 0


@dataclass(frozen=True)
class FileWriteFailed(OpactxEvent):
    type: str = "FileWriteFailed"
    path: Path | None = None
    message: str = ""


@dataclass(frozen=True)
class SchemaLoaded(OpactxEvent):
    type: str = "SchemaLoaded"
    path: Path | None = None


@dataclass(frozen=True)
class SchemaInvalid(OpactxEvent):
    type: str = "SchemaInvalid"
    path: Path | None = None
    message: str = ""


@dataclass(frozen=True)
class SchemaValidationFailed(OpactxEvent):
    type: str = "SchemaValidationFailed"
    schema_path: Path | None = None
    errors: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class SchemaValidationPassed(OpactxEvent):
    type: str = "SchemaValidationPassed"
    schema_path: Path | None = None


@dataclass(frozen=True)
class PluginResolved(OpactxEvent):
    type: str = "PluginResolved"
    kind: str = ""
    type_key: str = ""
    impl: str = ""


@dataclass(frozen=True)
class PluginMissing(OpactxEvent):
    type: str = "PluginMissing"
    kind: str = ""
    type_key: str = ""


@dataclass(frozen=True)
class Warning(OpactxEvent):
    type: str = "Warning"
    code: str = ""
    message: str = ""
    hint: str | None = None


@dataclass(frozen=True)
class Debug(OpactxEvent):
    type: str = "Debug"
    message: str = ""
    data: dict[str, Any] | None = None


@dataclass(frozen=True)
class SourceFetchStarted(OpactxEvent):
    type: str = "SourceFetchStarted"
    name: str = ""
    type_key: str = ""
    notes: str | None = None


@dataclass(frozen=True)
class SourceFetchCompleted(OpactxEvent):
    type: str = "SourceFetchCompleted"
    name: str = ""
    duration_ms: float = 0.0
    size_bytes: int | None = None


@dataclass(frozen=True)
class SourceFetchFailed(OpactxEvent):
    type: str = "SourceFetchFailed"
    name: str = ""
    duration_ms: float = 0.0
    message: str = ""
    hint: str | None = None
    type_key: str | None = None
    notes: str | None = None


@dataclass(frozen=True)
class SourcesPlanned(OpactxEvent):
    type: str = "SourcesPlanned"
    sources: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class BundleWriteStarted(OpactxEvent):
    type: str = "BundleWriteStarted"
    out_dir: Path | None = None


@dataclass(frozen=True)
class BundleWritten(OpactxEvent):
    type: str = "BundleWritten"
    out_dir: Path | None = None
    revision: str = ""
    files: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class BundleWriteFailed(OpactxEvent):
    type: str = "BundleWriteFailed"
    out_dir: Path | None = None
    message: str = ""


@dataclass(frozen=True)
class BundleOpened(OpactxEvent):
    type: str = "BundleOpened"
    path: Path | None = None


@dataclass(frozen=True)
class ManifestRead(OpactxEvent):
    type: str = "ManifestRead"
    path: Path | None = None
    revision: str | None = None
    roots: list[str] | None = None


@dataclass(frozen=True)
class DataRead(OpactxEvent):
    type: str = "DataRead"
    path: Path | None = None
    bytes: int = 0


@dataclass(frozen=True)
class ContextSummary(OpactxEvent):
    type: str = "ContextSummary"
    keys: list[str] = field(default_factory=list)
    counts: dict[str, int] | None = None


@dataclass(frozen=True)
class PathExtracted(OpactxEvent):
    type: str = "PathExtracted"
    path_pointer: str = ""
    value_type: str = ""
    preview: str = ""
    json_full: str | None = None


@dataclass(frozen=True)
class PluginsDiscovered(OpactxEvent):
    type: str = "PluginsDiscovered"
    kind: str = ""
    plugins: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class OpaStartPlanned(OpactxEvent):
    type: str = "OpaStartPlanned"
    address: str = ""
    bundle_path: Path | None = None
    policy_paths: list[Path] = field(default_factory=list)
    args: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OpaProcessStarted(OpactxEvent):
    type: str = "OpaProcessStarted"
    pid: int = 0


@dataclass(frozen=True)
class OpaStdout(OpactxEvent):
    type: str = "OpaStdout"
    line: str = ""


@dataclass(frozen=True)
class OpaStderr(OpactxEvent):
    type: str = "OpaStderr"
    line: str = ""


@dataclass(frozen=True)
class OpaProcessExited(OpactxEvent):
    type: str = "OpaProcessExited"
    exit_code: int = 0


def _serialize(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    return value
