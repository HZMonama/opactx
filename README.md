# opactx

opactx is a contract-first context compiler for Open Policy Agent (OPA).

It assembles intent plus source data into a deterministic `data.context`, validates it against JSON Schema, and writes an OPA bundle.

## Why opactx

OPA evaluates policy, but it does not enforce context contracts for you.

opactx exists to:

- prevent schema drift from silently breaking policy
- normalize mixed source payloads into one canonical context shape
- keep builds deterministic and auditable

## Build model

`opactx build` follows this flow:

1. Load config (`opactx.yaml`)
2. Load intent (`context/standards.yaml`, optional `context/exceptions.yaml`)
3. Fetch source payloads
4. Apply transforms
5. Validate final context against JSON Schema
6. Emit deterministic bundle output (`data.json`, `.manifest`)

Schema validation is always enforced in build.

## Built-in transforms

opactx ships built-in transforms for context assembly:

- `mount`
- `merge`
- `pick`
- `rename`
- `coerce`
- `defaults`
- `validate_schema`
- `ref_resolve`
- `sort_stable`
- `dedupe`
- `canonicalize` (compatibility/default canonical shape)

Full semantics and examples: `TRANSFORMS.md`.

## File roles

Use these roles for clarity:

- `opactx.yaml`
  Pipeline config: sources, transforms, schema path, output settings.
- `schema/context.schema.yaml`
  Human authoring format for the contract (recommended source of truth).
- `build/schema/context.schema.json`
  Compiled JSON Schema artifact used for machine validation; do not hand edit.

At build time, opactx validates against the configured schema path:

- `.yaml` / `.yml`: compiles DSL to JSON Schema first
- `.json`: uses JSON Schema directly

DSL reference: `SCHEMA_DSL.md`.
`opactx validate` runs DSL meta-schema validation before compile when schema is YAML.

## Bundle shape

`opactx build` writes:

```text
dist/bundle/
  data.json    # { "context": ... }
  .manifest    # revision + roots
```

## Install

```bash
pip install opactx
```

or:

```bash
pipx install opactx
```

## Quick start

```bash
opactx init --with-examples
opactx validate --strict
opactx build
opactx inspect dist/bundle
```

## Commands

| Command | Purpose |
| --- | --- |
| `opactx init` | Scaffold a project |
| `opactx validate` | Preflight config/schema checks (no source fetching) |
| `opactx build` | Compile context and produce OPA bundle |
| `opactx inspect` | Inspect bundle contents |
| `opactx run-opa` | Run OPA locally with a bundle |
| `opactx list-plugins` | List source/transform plugin entry points |

## Contributing

See `CONTRIBUTING.md`.
