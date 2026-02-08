from __future__ import annotations

import json
from pathlib import Path

import pytest

from opactx.transforms.builtin import BuiltinTransform, is_builtin_transform


def _base_intent() -> dict[str, object]:
    return {"standards": {"team": "platform"}, "exceptions": {}}


def _base_sources() -> dict[str, object]:
    return {
        "inventory": {
            "defaults": {"region": "global", "limits": {"cpu": 2}},
            "overrides": {"limits": {"cpu": 4}, "labels": ["blue"]},
            "repos": [
                {"id": "r2", "name": "zeta", "team_id": "t2"},
                {"id": "r1", "name": "alpha", "team_id": "t1"},
                {"id": "r1", "name": "alpha", "team_id": "t1"},
            ],
            "teams_by_id": {
                "t1": {"id": "t1", "name": "team-one"},
                "t2": {"id": "t2", "name": "team-two"},
            },
        }
    }


def _run(
    tmp_path: Path,
    *,
    name: str,
    context: dict[str, object],
    with_options: dict[str, object] | None = None,
    intent: dict[str, object] | None = None,
    sources: dict[str, object] | None = None,
) -> dict[str, object]:
    transform = BuiltinTransform(
        tmp_path,
        transform_name=name,
        intent=intent or _base_intent(),
        sources=sources or _base_sources(),
        **(with_options or {}),
    )
    return transform.apply(context)


def test_builtin_registry_contains_expected_names() -> None:
    names = {
        "canonicalize",
        "mount",
        "merge",
        "pick",
        "rename",
        "coerce",
        "defaults",
        "validate_schema",
        "ref_resolve",
        "sort_stable",
        "dedupe",
    }
    for name in names:
        assert is_builtin_transform(name) is True


def test_builtin_canonicalize_uses_intent_and_sources(tmp_path: Path) -> None:
    transform = BuiltinTransform(
        tmp_path,
        transform_name="canonicalize",
        intent=_base_intent(),
        sources=_base_sources(),
    )
    result = transform.apply({"ignored": True})
    assert result["standards"] == {"team": "platform"}
    assert "inventory" in result["sources"]


def test_mount_merges_into_context_path(tmp_path: Path) -> None:
    context = {"standards": {}, "exceptions": {}, "sources": {}, "config": {"limits": {"mem": 1}}}
    result = _run(
        tmp_path,
        name="mount",
        context=context,
        with_options={"source_id": "inventory", "target": "context.config"},
        sources={"inventory": {"limits": {"cpu": 4}, "labels": ["blue"]}},
    )
    assert result["config"]["limits"] == {"mem": 1, "cpu": 4}
    assert result["config"]["labels"] == ["blue"]


def test_merge_combines_objects_with_later_wins(tmp_path: Path) -> None:
    context = {
        "standards": {},
        "exceptions": {},
        "sources": {},
        "defaults": {"region": "global", "limits": {"cpu": 2, "mem": 1}},
        "overrides": {"limits": {"cpu": 6}},
    }
    result = _run(
        tmp_path,
        name="merge",
        context=context,
        with_options={
            "target": "context.request",
            "from": ["context.defaults", "context.overrides"],
        },
    )
    assert result["request"] == {"region": "global", "limits": {"cpu": 6, "mem": 1}}


def test_pick_selects_allowlisted_keys(tmp_path: Path) -> None:
    context = {
        "standards": {},
        "exceptions": {},
        "sources": {},
        "repo": {"team_id": "t1", "name": "svc", "owners": ["a"], "noise": "x"},
    }
    result = _run(
        tmp_path,
        name="pick",
        context=context,
        with_options={
            "path": "context.repo",
            "keys": ["team_id", "owners"],
        },
    )
    assert result["repo"] == {"team_id": "t1", "owners": ["a"]}


def test_rename_moves_values_between_paths(tmp_path: Path) -> None:
    context = {"standards": {}, "exceptions": {}, "sources": {}, "repo": {"team_id": "t1"}}
    result = _run(
        tmp_path,
        name="rename",
        context=context,
        with_options={"from": "context.repo.team_id", "to": "context.repo.team"},
    )
    assert "team_id" not in result["repo"]
    assert result["repo"]["team"] == "t1"


def test_coerce_normalizes_supported_types(tmp_path: Path) -> None:
    context = {
        "standards": {},
        "exceptions": {},
        "sources": {},
        "flags": {"enabled": "true"},
        "limits": {"count": "42", "ratio": "3.5"},
        "meta": {"at": "2026-01-01"},
    }
    result = _run(
        tmp_path,
        name="coerce",
        context=context,
        with_options={
            "rules": [
                {"path": "context.flags.enabled", "type": "bool"},
                {"path": "context.limits.count", "type": "int"},
                {"path": "context.limits.ratio", "type": "float"},
                {"path": "context.meta.at", "type": "timestamp"},
                {"path": "context.limits.count", "type": "string"},
            ]
        },
    )
    assert result["flags"]["enabled"] is True
    assert result["limits"]["count"] == "42"
    assert result["limits"]["ratio"] == 3.5
    assert result["meta"]["at"] == "2026-01-01T00:00:00Z"


def test_defaults_only_applies_when_missing(tmp_path: Path) -> None:
    context = {"standards": {}, "exceptions": {}, "sources": {}, "env": "prod", "request": {}}
    result = _run(
        tmp_path,
        name="defaults",
        context=context,
        with_options={
            "values": {
                "context.env": "dev",
                "context.request.region": "global",
            }
        },
    )
    assert result["env"] == "prod"
    assert result["request"]["region"] == "global"


def test_validate_schema_checks_current_context(tmp_path: Path) -> None:
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["standards", "exceptions", "sources", "env"],
        "properties": {
            "standards": {"type": "object"},
            "exceptions": {"type": "object"},
            "sources": {"type": "object"},
            "env": {"type": "string"},
        },
        "additionalProperties": True,
    }
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    transform = BuiltinTransform(
        tmp_path,
        transform_name="validate_schema",
        schema_path=schema_path,
    )
    result = transform.apply({"standards": {}, "exceptions": {}, "sources": {}, "env": "dev"})
    assert result["env"] == "dev"


def test_ref_resolve_attaches_lookup_objects(tmp_path: Path) -> None:
    context = {
        "standards": {},
        "exceptions": {},
        "sources": {},
        "repos": [
            {"id": "r1", "team_id": "t1"},
            {"id": "r2", "team_id": "t2"},
        ],
        "teams_by_id": {
            "t1": {"id": "t1", "name": "one"},
            "t2": {"id": "t2", "name": "two"},
        },
    }
    result = _run(
        tmp_path,
        name="ref_resolve",
        context=context,
        with_options={
            "rules": [
                {
                    "items": "context.repos",
                    "lookup": "context.teams_by_id",
                    "ref_key": "team_id",
                    "target_key": "team",
                    "required": True,
                }
            ]
        },
    )
    assert result["repos"][0]["team"]["name"] == "one"
    assert result["repos"][1]["team"]["name"] == "two"


def test_sort_stable_orders_array_by_key(tmp_path: Path) -> None:
    context = {
        "standards": {},
        "exceptions": {},
        "sources": {},
        "repos": [
            {"id": "b", "name": "alpha"},
            {"id": "a", "name": "alpha"},
            {"id": "c", "name": "zeta"},
        ],
    }
    result = _run(
        tmp_path,
        name="sort_stable",
        context=context,
        with_options={"path": "context.repos", "by": "name"},
    )
    assert [repo["id"] for repo in result["repos"]] == ["b", "a", "c"]


def test_dedupe_removes_duplicates_by_key(tmp_path: Path) -> None:
    context = {
        "standards": {},
        "exceptions": {},
        "sources": {},
        "repos": [
            {"id": "a", "name": "one"},
            {"id": "a", "name": "one-dup"},
            {"id": "b", "name": "two"},
        ],
    }
    result = _run(
        tmp_path,
        name="dedupe",
        context=context,
        with_options={"path": "context.repos", "by": "id"},
    )
    assert [repo["id"] for repo in result["repos"]] == ["a", "b"]


def test_unknown_builtin_transform_raises(tmp_path: Path) -> None:
    transform = BuiltinTransform(tmp_path, transform_name="unknown_transform")
    with pytest.raises(ValueError, match="Unknown builtin transform"):
        transform.apply({})
