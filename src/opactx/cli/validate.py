from __future__ import annotations

import json
from pathlib import Path

import typer
from rich.console import Console

from opactx.core.validate import ValidationReport, validate_project

console = Console()


def validate(
    config: Path = typer.Option(
        Path("opactx.yaml"),
        "--config",
        "-c",
        help="Path to opactx.yaml.",
    ),
    project: Path = typer.Option(
        Path("."),
        "--project",
        "-p",
        help="Base directory for relative paths.",
    ),
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Require that referenced plugins resolve.",
    ),
    check_schema: bool = typer.Option(
        True,
        "--check-schema/--no-check-schema",
        help="Validate a candidate context against the schema.",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Show stack traces for unexpected errors.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Emit machine-readable JSON report.",
    ),
) -> None:
    def _log(message: str) -> None:
        if not json_output:
            console.print(message)

    try:
        report = validate_project(
            project,
            config_path=config,
            strict=strict,
            check_schema=check_schema,
            on_step=_log,
        )
    except Exception as exc:  # noqa: BLE001
        if debug:
            raise
        console.print(f"[red]Unexpected error:[/red] {exc}")
        raise typer.Exit(code=2)

    if json_output:
        _emit_json(report)
    else:
        _emit_human(report, strict=strict)

    raise typer.Exit(code=0 if report.ok else 2)


def _emit_human(report: ValidationReport, *, strict: bool) -> None:
    _print_check("Config", report.checks.get("config"))
    _print_check("Schema", report.checks.get("schema"))
    _print_check("Intent", report.checks.get("intent"))

    plugins_status = report.checks.get("plugins")
    if plugins_status == "skipped" and not strict:
        console.print("Plugins: skipped (not strict)")
    else:
        _print_check("Plugins", plugins_status)

    schema_check = report.checks.get("schema_check")
    if schema_check == "partial":
        console.print("Schema check: partial")
    else:
        _print_check("Schema check", schema_check)

    for warning in report.warnings:
        console.print(f"[yellow]Warning:[/yellow] {warning}")
    for error in report.errors:
        message = error["message"]
        path = error.get("path")
        if path:
            message = f"{message} ({path})"
        console.print(f"[red]Error:[/red] {message}")


def _print_check(label: str, status: str | None) -> None:
    status = status or "skipped"
    if status == "ok":
        console.print(f"{label} OK")
    elif status == "failed":
        console.print(f"{label} FAILED")
    else:
        console.print(f"{label}: {status}")


def _emit_json(report: ValidationReport) -> None:
    payload = {
        "ok": report.ok,
        "checks": report.checks,
        "warnings": report.warnings,
        "errors": report.errors,
    }
    console.print(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
    )
