from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Sequence


class ExecSource:
    def __init__(
        self,
        project_dir: Path,
        *,
        cmd: Sequence[str],
        timeout_s: float | None = None,
        **_: Any,
    ):
        if not isinstance(cmd, (list, tuple)) or not all(isinstance(item, str) for item in cmd):
            raise ValueError("exec source requires cmd as a list of strings")
        self.cmd = list(cmd)
        self.timeout = timeout_s
        self.project_dir = project_dir

    def fetch(self) -> Any:
        result = subprocess.run(
            self.cmd,
            cwd=self.project_dir,
            capture_output=True,
            text=True,
            check=False,
            timeout=self.timeout,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise RuntimeError(f"Command failed with exit code {result.returncode}: {stderr}")
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Command output is not valid JSON.") from exc
