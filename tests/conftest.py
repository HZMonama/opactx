from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def sample_project(tmp_path: Path) -> Path:
    project_dir = tmp_path / "project"
    project_dir.mkdir(parents=True)
    (project_dir / "context").mkdir()
    (project_dir / "schema").mkdir()
    (project_dir / "fixtures").mkdir()

    (project_dir / "opactx.yaml").write_text(
        """
version: v1

schema: schema/context.schema.json
context_dir: context

sources:
  - name: inventory
    type: file
    with:
      path: fixtures/inventory.json

transforms:
  - name: canonicalize
    type: builtin
    with: {}

output:
  dir: dist/bundle
  include_policy: false
  tarball: false
""".strip()
        + "\n",
        encoding="utf-8",
    )

    (project_dir / "schema" / "context.schema.json").write_text(
        """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["standards", "exceptions", "sources"],
  "properties": {
    "standards": { "type": "object" },
    "exceptions": { "type": "object" },
    "sources": { "type": "object" }
  },
  "additionalProperties": false
}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    (project_dir / "context" / "standards.yaml").write_text(
        """
allowed_regions:
  - us-east-1
  - eu-west-1
approved_registries:
  - ghcr.io/my-org
  - registry.example.com
""".strip()
        + "\n",
        encoding="utf-8",
    )

    (project_dir / "context" / "exceptions.yaml").write_text(
        """
exceptions: []
""".strip()
        + "\n",
        encoding="utf-8",
    )

    (project_dir / "fixtures" / "inventory.json").write_text(
        """
{
  "resources": [
    { "id": "i-123", "region": "us-east-1" },
    { "id": "i-456", "region": "ap-south-1" }
  ]
}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    return project_dir
