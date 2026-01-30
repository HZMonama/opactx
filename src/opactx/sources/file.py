import json
from pathlib import Path
from typing import Any


class FileSource:
    def __init__(self, project_dir: Path, *, path: str, **_: Any):
        self.path = project_dir / path

    def fetch(self) -> Any:
        return json.loads(self.path.read_text(encoding="utf-8"))
