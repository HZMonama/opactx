from __future__ import annotations

import time
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Iterable

from opactx.core import events as ev

TEMPLATE_PACKAGE = "opactx.templates.scaffold"


@dataclass(frozen=True)
class ScaffoldFile:
    template_path: Path
    destination_path: Path
    substitutions: dict[str, str] | None = None


def init_events(
    *,
    project: Path,
    force: bool,
    minimal: bool,
    with_examples: bool,
    dry_run: bool,
    name: str | None,
    no_policy: bool,
    schema: str,
) -> Iterable[ev.OpactxEvent]:
    options = {
        "force": force,
        "minimal": minimal,
        "with_examples": with_examples,
        "dry_run": dry_run,
        "name": name,
        "no_policy": no_policy,
        "schema": schema,
    }
    yield ev.CommandStarted(
        command="init",
        project_dir=project,
        config_path=project / "opactx.yaml",
        options=options,
    )

    started = time.perf_counter()
    yield ev.StageStarted(command="init", stage_id="resolve_target", label="Resolve target directory")
    schema = schema.lower()
    if schema not in {"jsonschema", "openapi"}:
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="init",
            stage_id="resolve_target",
            duration_ms=duration_ms,
            error_code="invalid_schema",
            message=f"Unknown schema type: {schema}",
        )
        yield ev.CommandCompleted(command="init", ok=False, exit_code=2)
        return
    if schema == "openapi":
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="init",
            stage_id="resolve_target",
            duration_ms=duration_ms,
            error_code="unsupported_schema",
            message="OpenAPI scaffolding is not supported yet.",
        )
        yield ev.CommandCompleted(command="init", ok=False, exit_code=2)
        return
    if project.exists() and project.is_file():
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="init",
            stage_id="resolve_target",
            duration_ms=duration_ms,
            error_code="invalid_path",
            message="Target path exists and is a file.",
        )
        yield ev.CommandCompleted(command="init", ok=False, exit_code=2)
        return

    if minimal and with_examples:
        with_examples = False
        yield ev.Warning(
            command="init",
            code="examples_ignored",
            message="Ignoring --with-examples because --minimal was set.",
        )

    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(command="init", stage_id="resolve_target", duration_ms=duration_ms)

    started = time.perf_counter()
    yield ev.StageStarted(command="init", stage_id="plan_scaffold", label="Plan scaffold")
    project_name = name or project.resolve().name
    files = _scaffold_files(
        minimal=minimal,
        with_examples=with_examples,
        no_policy=no_policy,
        project_name=project_name,
    )
    actions = _plan_actions(project, files, force=force)
    for action, destination in actions:
        yield ev.FilePlanned(command="init", op=action, path=destination)
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(command="init", stage_id="plan_scaffold", duration_ms=duration_ms)

    started = time.perf_counter()
    yield ev.StageStarted(command="init", stage_id="apply_scaffold", label="Apply scaffold")
    if dry_run:
        duration_ms = _elapsed_ms(started)
        yield ev.StageCompleted(
            command="init",
            stage_id="apply_scaffold",
            duration_ms=duration_ms,
            status="skipped",
        )
        yield ev.CommandCompleted(command="init", ok=True, exit_code=0)
        return
    try:
        project.mkdir(parents=True, exist_ok=True)
        file_map = {project / f.destination_path: f for f in files}
        for action, destination in actions:
            if action == "SKIP":
                continue
            scaffold = file_map[destination]
            destination.parent.mkdir(parents=True, exist_ok=True)
            content = _render_template(scaffold)
            destination.write_text(content, encoding="utf-8")
            yield ev.FileWritten(
                command="init",
                path=destination,
                bytes=len(content.encode("utf-8")),
            )
    except OSError as exc:
        duration_ms = _elapsed_ms(started)
        yield ev.FileWriteFailed(command="init", path=project, message=str(exc))
        yield ev.StageFailed(
            command="init",
            stage_id="apply_scaffold",
            duration_ms=duration_ms,
            error_code="write_failed",
            message=f"Failed to write scaffold: {exc}",
        )
        yield ev.CommandCompleted(command="init", ok=False, exit_code=2)
        return

    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(command="init", stage_id="apply_scaffold", duration_ms=duration_ms)
    yield ev.CommandCompleted(command="init", ok=True, exit_code=0)


def _scaffold_files(
    *,
    minimal: bool,
    with_examples: bool,
    no_policy: bool,
    project_name: str,
) -> list[ScaffoldFile]:
    files: list[ScaffoldFile] = []

    if minimal:
        template_name = "opactx.minimal.yaml"
    elif with_examples:
        template_name = "opactx.yaml"
    else:
        template_name = "opactx.no-examples.yaml"

    files.append(
        ScaffoldFile(
            template_path=Path(template_name),
            destination_path=Path("opactx.yaml"),
        )
    )

    files.append(
        ScaffoldFile(
            template_path=Path("schema") / "context.schema.json",
            destination_path=Path("schema") / "context.schema.json",
        )
    )
    files.append(
        ScaffoldFile(
            template_path=Path("context") / "standards.yaml",
            destination_path=Path("context") / "standards.yaml",
        )
    )

    if not minimal:
        files.append(
            ScaffoldFile(
                template_path=Path("context") / "exceptions.yaml",
                destination_path=Path("context") / "exceptions.yaml",
            )
        )

    if not minimal:
        files.append(
            ScaffoldFile(
                template_path=Path("README.md"),
                destination_path=Path("README.md"),
                substitutions={"{{PROJECT_NAME}}": project_name},
            )
        )

    if not minimal and not no_policy:
        files.append(
            ScaffoldFile(
                template_path=Path("policy") / "README.md",
                destination_path=Path("policy") / "README.md",
            )
        )
        if with_examples:
            files.append(
                ScaffoldFile(
                    template_path=Path("policy") / "example.rego",
                    destination_path=Path("policy") / "example.rego",
                )
            )

    if with_examples:
        files.append(
            ScaffoldFile(
                template_path=Path("fixtures") / "inventory.json",
                destination_path=Path("fixtures") / "inventory.json",
            )
        )

    return files


def _plan_actions(
    project: Path,
    files: Iterable[ScaffoldFile],
    *,
    force: bool,
) -> list[tuple[str, Path]]:
    actions: list[tuple[str, Path]] = []
    for scaffold in files:
        destination = project / scaffold.destination_path
        if destination.exists():
            if force:
                actions.append(("OVERWRITE", destination))
            else:
                actions.append(("SKIP", destination))
        else:
            actions.append(("CREATE", destination))
    return actions


def _render_template(scaffold: ScaffoldFile) -> str:
    template_root = resources.files(TEMPLATE_PACKAGE)
    content = (template_root / scaffold.template_path).read_text(encoding="utf-8")
    if scaffold.substitutions:
        for key, value in scaffold.substitutions.items():
            content = content.replace(key, value)
    return content


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000
