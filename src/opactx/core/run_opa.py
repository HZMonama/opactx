from __future__ import annotations

import queue
import subprocess
import threading
import time
from pathlib import Path
from typing import Iterable

from opactx.config.load import ConfigError, load_config
from opactx.core import events as ev
from opactx.core.build import build_events


def run_opa_events(
    *,
    project_dir: Path,
    config_path: Path | None = None,
    bundle_path: Path | None = None,
    policy_path: Path | None = None,
    address: str = "http://localhost:8181",
    skip_build: bool = False,
) -> Iterable[ev.OpactxEvent]:
    project_dir = project_dir.resolve()
    config_path = config_path or Path("opactx.yaml")
    if not config_path.is_absolute():
        config_path = project_dir / config_path

    options = {
        "bundle_path": str(bundle_path) if bundle_path else None,
        "policy_path": str(policy_path) if policy_path else None,
        "address": address,
        "skip_build": skip_build,
    }
    yield ev.CommandStarted(
        command="run-opa",
        project_dir=project_dir,
        config_path=config_path,
        options=options,
    )

    yield ev.StageStarted(command="run-opa", stage_id="prepare_bundle", label="Prepare bundle")
    started = time.perf_counter()
    resolved_bundle = _resolve_bundle_path(
        project_dir, config_path, bundle_path=bundle_path, skip_build=skip_build
    )
    if isinstance(resolved_bundle, str):
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="run-opa",
            stage_id="prepare_bundle",
            duration_ms=duration_ms,
            error_code="bundle_error",
            message=resolved_bundle,
        )
        yield ev.CommandCompleted(command="run-opa", ok=False, exit_code=2)
        return

    if not skip_build:
        ok = _run_build(project_dir, config_path, resolved_bundle)
        if not ok:
            duration_ms = _elapsed_ms(started)
            yield ev.StageFailed(
                command="run-opa",
                stage_id="prepare_bundle",
                duration_ms=duration_ms,
                error_code="build_failed",
                message="Failed to build bundle.",
            )
            yield ev.CommandCompleted(command="run-opa", ok=False, exit_code=2)
            return
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="run-opa",
        stage_id="prepare_bundle",
        duration_ms=duration_ms,
        status="success",
    )

    if not resolved_bundle.exists():
        yield ev.StageFailed(
            command="run-opa",
            stage_id="prepare_bundle",
            duration_ms=0.0,
            error_code="bundle_missing",
            message=f"Bundle not found: {resolved_bundle}",
        )
        yield ev.CommandCompleted(command="run-opa", ok=False, exit_code=2)
        return

    resolved_policy = _resolve_policy_path(project_dir, policy_path)
    cmd = _opa_command(resolved_bundle, resolved_policy, address)

    yield ev.StageStarted(command="run-opa", stage_id="start_opa", label="Start OPA")
    started = time.perf_counter()
    yield ev.OpaStartPlanned(
        command="run-opa",
        address=address,
        bundle_path=resolved_bundle,
        policy_paths=[resolved_policy],
        args=cmd,
    )

    try:
        process = subprocess.Popen(  # noqa: S603
            cmd,
            cwd=project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except OSError as exc:
        duration_ms = _elapsed_ms(started)
        yield ev.StageFailed(
            command="run-opa",
            stage_id="start_opa",
            duration_ms=duration_ms,
            error_code="opa_exec",
            message=str(exc),
        )
        yield ev.CommandCompleted(command="run-opa", ok=False, exit_code=2)
        return

    yield ev.OpaProcessStarted(command="run-opa", pid=process.pid or 0)
    duration_ms = _elapsed_ms(started)
    yield ev.StageCompleted(
        command="run-opa",
        stage_id="start_opa",
        duration_ms=duration_ms,
        status="success",
    )

    yield ev.StageStarted(command="run-opa", stage_id="stream_output", label="Stream output")
    started = time.perf_counter()

    events_queue: queue.Queue[tuple[str, str]] = queue.Queue()

    def _drain(pipe, label: str) -> None:
        if pipe is None:
            return
        for line in iter(pipe.readline, ""):
            events_queue.put((label, line.rstrip()))
        pipe.close()

    stdout_thread = threading.Thread(target=_drain, args=(process.stdout, "stdout"), daemon=True)
    stderr_thread = threading.Thread(target=_drain, args=(process.stderr, "stderr"), daemon=True)
    stdout_thread.start()
    stderr_thread.start()

    try:
        while True:
            try:
                label, line = events_queue.get(timeout=0.1)
                if label == "stderr":
                    yield ev.OpaStderr(command="run-opa", line=line)
                else:
                    yield ev.OpaStdout(command="run-opa", line=line)
            except queue.Empty:
                pass
            if process.poll() is not None and events_queue.empty():
                if not stdout_thread.is_alive() and not stderr_thread.is_alive():
                    break
    except KeyboardInterrupt:
        process.terminate()
    finally:
        exit_code = process.wait()

    duration_ms = _elapsed_ms(started)
    if exit_code == 0:
        yield ev.StageCompleted(
            command="run-opa",
            stage_id="stream_output",
            duration_ms=duration_ms,
            status="success",
        )
    else:
        yield ev.StageFailed(
            command="run-opa",
            stage_id="stream_output",
            duration_ms=duration_ms,
            error_code="opa_exit",
            message=f"OPA exited with code {exit_code}",
        )
    yield ev.OpaProcessExited(command="run-opa", exit_code=exit_code)
    yield ev.CommandCompleted(command="run-opa", ok=exit_code == 0, exit_code=0 if exit_code == 0 else 2)


def _resolve_bundle_path(
    project_dir: Path,
    config_path: Path,
    *,
    bundle_path: Path | None,
    skip_build: bool,
) -> Path | str:
    if bundle_path:
        return bundle_path if bundle_path.is_absolute() else project_dir / bundle_path
    try:
        config = load_config(project_dir, config_path)
    except (ConfigError, ValueError) as exc:
        return str(exc)
    output_dir = Path(config.output.dir)
    if not output_dir.is_absolute():
        output_dir = project_dir / output_dir
    return output_dir


def _resolve_policy_path(project_dir: Path, policy_path: Path | None) -> Path:
    policy = policy_path or Path("policy")
    return policy if policy.is_absolute() else project_dir / policy


def _opa_command(bundle: Path, policy: Path, address: str) -> list[str]:
    cmd = ["opa", "run", "--server", "--addr", address, "--bundle", str(bundle)]
    if policy.exists():
        cmd.append(str(policy))
    return cmd


def _run_build(project_dir: Path, config_path: Path, output_dir: Path) -> bool:
    ok = True
    for event in build_events(
        project_dir=project_dir,
        config_path=config_path,
        output_dir=output_dir,
        clean=False,
        dry_run=False,
        fail_fast=True,
        debug=False,
    ):
        if isinstance(event, ev.CommandCompleted):
            ok = event.ok
    return ok


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000
