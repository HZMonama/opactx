from __future__ import annotations

from typing import Any


def canonicalize(intent: dict[str, Any], sources: dict[str, Any]) -> dict[str, Any]:
    return {
        "standards": intent.get("standards", {}),
        "exceptions": intent.get("exceptions", {}),
        "sources": sources,
    }


class CanonicalizeTransform:
    def __init__(self, project_dir: object, **_: Any):
        self.project_dir = project_dir

    def apply(self, value: dict[str, Any]) -> dict[str, Any]:
        intent = value.get("intent", {})
        sources = value.get("sources", {})
        return canonicalize(intent, sources)
