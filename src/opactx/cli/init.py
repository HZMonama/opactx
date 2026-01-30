from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Iterable

import typer
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

TEMPLATE_PACKAGE = "opactx.templates.scaffold"
console = Console()


@dataclass(frozen=True)
class ScaffoldFile:
    template_path: Path
    destination_path: Path
    substitutions: dict[str, str] | None = None


def init(
    project: Path = typer.Argument(
        Path("."),
        help="Directory to initialize (created if missing).",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite existing scaffold files.",
    ),
    minimal: bool = typer.Option(
        False,
        "--minimal",
        help="Generate only the minimal required files to run opactx build.",
    ),
    with_examples: bool = typer.Option(
        False,
        "--with-examples",
        help="Include example fixtures and sample policy module(s).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Do not write files; print planned operations.",
    ),
    name: str | None = typer.Option(
        None,
        "--name",
        help="Optional project name for README title.",
    ),
    no_policy: bool = typer.Option(
        False,
        "--no-policy",
        help="Do not scaffold policy/ directory.",
    ),
    schema: str = typer.Option(
        "jsonschema",
        "--schema",
        help="Contract schema type (jsonschema or openapi).",
    ),
) -> None:
    """Initialize an opactx project scaffold."""
    schema = schema.lower()
    if schema not in {"jsonschema", "openapi"}:
        typer.secho(f"Unknown schema type: {schema}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)
    if schema == "openapi":
        typer.secho("OpenAPI scaffolding is not supported yet.", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)

    if project.exists() and project.is_file():
        typer.secho("Target path exists and is a file.", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)

    if minimal and with_examples:
        typer.secho(
            "Ignoring --with-examples because --minimal was set.",
            err=True,
            fg=typer.colors.YELLOW,
        )
        with_examples = False

    project_name = name or project.resolve().name

    files = _scaffold_files(
        minimal=minimal,
        with_examples=with_examples,
        no_policy=no_policy,
        project_name=project_name,
    )

    actions = _plan_actions(project, files, force=force)
    _print_actions_table(actions, _init_label(minimal, with_examples, no_policy))

    if dry_run:
        _print_summary(project)
        raise typer.Exit(code=0)

    try:
        project.mkdir(parents=True, exist_ok=True)
        _write_files(project, actions, files)
    except OSError as exc:
        typer.secho(f"Failed to write scaffold: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)

    _print_summary(project)


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


def _write_files(
    project: Path,
    actions: Iterable[tuple[str, Path]],
    files: Iterable[ScaffoldFile],
) -> None:
    file_map = {project / f.destination_path: f for f in files}
    for action, destination in actions:
        if action == "SKIP":
            continue
        scaffold = file_map[destination]
        destination.parent.mkdir(parents=True, exist_ok=True)
        content = _render_template(scaffold)
        destination.write_text(content, encoding="utf-8")


def _render_template(scaffold: ScaffoldFile) -> str:
    template_root = resources.files(TEMPLATE_PACKAGE)
    content = (template_root / scaffold.template_path).read_text(encoding="utf-8")
    if scaffold.substitutions:
        for key, value in scaffold.substitutions.items():
            content = content.replace(key, value)
    return content


def _print_summary(project: Path) -> None:
    console.print("")
    console.print(f"[green]✓ Successfully initialized:[/green] {project}")
    console.print("")

    steps = "\n".join(
        [
            "- opactx build (outputs to dist/bundle/)",
            "- Edit opactx.yaml sources and context/standards.yaml",
        ]
    )
    console.print(
        Panel(steps, title="Next steps", box=box.ROUNDED, expand=False, title_align="left")
    )


def _print_actions_table(actions: Iterable[tuple[str, Path]], title: str) -> None:
    console.print("")
    table = Table(title=title, box=box.ROUNDED, title_justify="left")
    table.add_column("Action", style="bold")
    table.add_column("Path", overflow="fold")
    table.add_column("Note", style="dim")

    for action, destination in actions:
        style = {
            "CREATE": "green",
            "OVERWRITE": "yellow",
            "SKIP": "dim",
        }.get(action, "")
        if action == "CREATE":
            note = "new"
        elif action == "SKIP":
            note = "exists"
        else:
            note = ""
        table.add_row(action, str(destination), note, style=style)

    console.print(table)


def _init_label(minimal: bool, with_examples: bool, no_policy: bool) -> str:
    if minimal:
        return "Initializing minimal template"
    if with_examples:
        return "Initializing template with examples"
    if no_policy:
        return "Initializing template without policy"
    return "Initializing standard template"
