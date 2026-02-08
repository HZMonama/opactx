# {{PROJECT_NAME}}

opactx is a contract-first context compiler that produces OPA bundles. This project
defines a schema and curated context inputs for policy evaluation.
Docs: https://opactx.gitbook.io/opactx/

## Quickstart

1) Review `opactx.yaml` and adjust sources.
2) Edit `schema/` and `context/` to match your standards.
3) Build the bundle:

```bash
opactx build
```

Bundle output: `dist/bundle/`.

## Project layout

- `opactx.yaml` - build configuration and sources.
- `schema/context.schema.yaml` - authoring DSL for the context contract (or `.json` with `--json-schema`).
- `context/` - curated intent inputs merged into the policy-facing contract.
