## opactx

opactx is a contract-first context compiler that produces OPA bundles. It reads
human-authored YAML intent plus external sources, validates the final context
against a JSON Schema, and writes a deterministic bundle.

## Quickstart

```bash
opactx init .
opactx build
```

Bundle output: `dist/bundle/` with `data.json` and `.manifest`.

## Build command

Common options:

```bash
opactx build --config opactx.yaml --project . --output-dir dist/bundle --clean
```

## Validate command

Preflight checks without fetching sources:

```bash
opactx validate --config opactx.yaml --project . --strict
```

## Context YAML

`context/standards.yaml` and `context/exceptions.yaml` must be YAML mappings.
If `exceptions.yaml` is missing, it is treated as `{}`. These files feed:

```json
{
  "context": {
    "standards": { ... },
    "exceptions": { ... },
    "sources": { ... }
  }
}
```

## Config (opactx.yaml)

Required keys:

- `version: "v1"`
- `schema: <path to JSON Schema>`
- `context_dir: <path>`
- `output.dir: <path>`

Optional keys:

- `sources: []`
- `transforms: []`
- `output.include_policy`
- `output.tarball`

Example:

```yaml
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
```
