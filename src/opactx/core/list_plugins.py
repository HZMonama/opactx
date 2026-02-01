from __future__ import annotations

import time
from importlib.metadata import entry_points
from typing import Iterable

from opactx.core import events as ev


def list_plugins_events() -> Iterable[ev.OpactxEvent]:
    yield ev.CommandStarted(command="list-plugins")

    started = time.perf_counter()
    yield ev.StageStarted(command="list-plugins", stage_id="discover_sources", label="Discover sources")
    sources = _discover("opactx.sources")
    yield ev.PluginsDiscovered(command="list-plugins", kind="sources", plugins=sources)
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="list-plugins",
        stage_id="discover_sources",
        duration_ms=duration_ms,
        status="success",
    )

    started = time.perf_counter()
    yield ev.StageStarted(command="list-plugins", stage_id="discover_transforms", label="Discover transforms")
    transforms = _discover("opactx.transforms")
    yield ev.PluginsDiscovered(command="list-plugins", kind="transforms", plugins=transforms)
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="list-plugins",
        stage_id="discover_transforms",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.CommandCompleted(command="list-plugins", ok=True, exit_code=0)


def _discover(group: str) -> list[dict[str, str]]:
    plugins: list[dict[str, str]] = []
    for ep in entry_points(group=group):
        plugins.append({"type_key": ep.name, "impl": ep.value})
    return plugins


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000
