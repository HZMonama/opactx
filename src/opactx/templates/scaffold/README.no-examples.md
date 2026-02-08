# {{PROJECT_NAME}}

This repository is an opactx project scaffold.
Docs: https://opactx.gitbook.io/opactx/

## What to edit

- `opactx.yaml` - pipeline configuration (sources, transforms, output, schema path).
- `schema/context.schema.yaml` - context contract authored in the schema DSL (or `.json` with `--json-schema`).
- `context/standards.yaml` - required policy-facing context values.
- `context/exceptions.yaml` - optional exception entries.
- `policy/*.rego` - policy modules.

## Next

1) Fill in the starter files.
2) Run `opactx validate`.
3) Run `opactx build`.
