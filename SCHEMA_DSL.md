# `context.schema.yaml` DSL (v0.1)

## Purpose

`context.schema.yaml` is the human-authored contract format for `data.context`.
opactx compiles it to JSON Schema (Draft 2020-12) and validates the compiled output.

`schema/context.schema.yaml` is the source of truth.
`build/schema/context.schema.json` is a generated artifact.

## Validation flow in `opactx validate`

When schema path is YAML (`.yaml` / `.yml`), `opactx validate` does:

1. Validate DSL document against the DSL meta-schema.
2. Compile DSL to JSON Schema.
3. Validate the compiled JSON Schema as a JSON Schema document.
4. Run schema checks against intent-only candidate context.

No source fetching occurs in `validate`.

## Top-level shape

```yaml
dsl: opactx.schema/v1
id: context
title: Policy Context
description: Canonical context contract used as data.context
root: context
strict: true
definitions: {}
schema:
  type: object
  fields: {}
```

Required top-level keys:

- `dsl`
- `id`
- `title`
- `description`
- `root`
- `schema`

Optional top-level keys:

- `strict` (default `true`)
- `definitions`

Rules:

- `dsl` must be `opactx.schema/v1`.
- `schema.type` must be `object` in v0.1.
- Unknown top-level keys are rejected.

## Supported types

Base types:

- `object`
- `array`
- `string`
- `number`
- `integer`
- `boolean`
- `null`

Supported string formats:

- `date-time`
- `email`
- `uri`
- `uuid`

## Common node keywords

Supported on schema nodes (with context-specific restrictions):

- `type`
- `description`
- `required` (field nodes)
- `nullable`
- `default`
- `examples`
- `deprecated`
- `tags`

Compilation notes:

- `nullable: true` compiles to `type: [<base>, "null"]`.
- `tags` compiles to `x-opactx-tags`.
- `default` and `examples` are type-checked by opactx during compile.

## Object nodes

Keywords:

- `type: object`
- `fields`
- `strict`
- `allow_empty_object`

Rules:

- `fields` is required unless `allow_empty_object: true`.
- If top-level `strict` is true, strictness is inherited recursively unless overridden.

Compilation:

- `fields` -> `properties`
- field `required: true` -> parent `required`
- `strict: true` -> `additionalProperties: false`
- `strict: false` -> `additionalProperties: true`

## Array nodes

Keywords:

- `type: array`
- `items` (required)
- `min_items`
- `max_items`
- `unique_by`

Compilation:

- `items` -> `items`
- `min_items` -> `minItems`
- `max_items` -> `maxItems`
- `unique_by` -> `x-opactx-uniqueBy` (extension metadata)

Validation:

- `min_items` and `max_items` must be non-negative integers.
- `min_items <= max_items`.

## Scalar nodes

String:

- `min_len` -> `minLength`
- `max_len` -> `maxLength`
- `pattern` -> `pattern`
- `enum` -> `enum`
- `format` -> `format`

Number / Integer:

- `min` -> `minimum`
- `max` -> `maximum`
- `enum` -> `enum`

Boolean / Null:

- `enum` supported

Validation:

- `min_len <= max_len`
- `min <= max`
- `enum` values must match node type

## Required semantics

`required: true` is defined on field nodes, not in separate lists.
The compiler collects required fields into the parent object `required` array.

If an object itself must exist, mark the object field as `required: true`.

## Reuse with definitions and refs

Reusable types are declared in `definitions`.

Accepted ref forms:

- `#/definitions/<Name>`
- `#/$defs/<Name>`

Compilation emits refs as:

- `#/$defs/<Name>`

Rules:

- ref target must exist
- ref cycles across definitions are rejected
- `$ref` node may only contain:
  - `$ref`
  - optional `description`
  - optional `deprecated`
  - optional `required` when used as a field node

## Meta-schema

The DSL meta-schema used for structural validation is embedded in:

- `src/opactx/schema/meta_schema.py`

It enforces:

- top-level keys and required keys
- node family shape (`object`, `array`, scalar, `$ref`)
- allowed keys per node type
- base structural constraints (for example, array `items` required)

Semantic rules (type-checked defaults/examples/enums, strictness inheritance, ref existence/cycles, min/max relations) are enforced by the compiler.

## Artifact output

When build uses a YAML schema path, opactx emits compiled JSON Schema to:

- `build/schema/<dsl_filename>.json`

Example:

- `schema/context.schema.yaml` -> `build/schema/context.schema.json`

## Minimal example

```yaml
dsl: opactx.schema/v1
id: context
title: Policy Context
description: Canonical context contract used as data.context
root: context
strict: true

definitions:
  Team:
    type: object
    fields:
      id:
        type: string
        required: true
      name:
        type: string
        required: true

schema:
  type: object
  fields:
    env:
      type: string
      required: true
      enum: [dev, staging, prod]
    teams:
      type: array
      items:
        $ref: "#/definitions/Team"
      unique_by: id
```

## Bad vs good schemas

### Core rule

If a field appears in `context.schema.yaml`, policies should be expected to depend on its shape.

If policies do not care about a field's structure, it likely does not belong in the contract.

### Anti-pattern: opaque plumbing objects

This is valid DSL but a poor contract:

```yaml
schema:
  type: object
  fields:
    standards:
      type: object
      required: true
      strict: false
      allow_empty_object: true
    exceptions:
      type: object
      required: true
      strict: false
      allow_empty_object: true
    sources:
      type: object
      required: true
      strict: false
      allow_empty_object: true
```

Why this is weak:

- It models opactx plumbing boundaries instead of policy-facing domain data.
- It does not enforce meaningful invariants inside fields.
- Drift can still leak through as opaque objects.

### Better exemplar: policy-facing domain contract

```yaml
dsl: opactx.schema/v1
id: context
title: Policy Context
description: Canonical context consumed by policies
root: context
strict: true

schema:
  type: object
  fields:
    env:
      type: string
      required: true
      enum: [dev, staging, prod]

    actor:
      type: object
      required: true
      fields:
        id: { type: string, required: true }
        role:
          type: string
          required: true
          enum: [user, admin, service]

    request:
      type: object
      required: true
      fields:
        action: { type: string, required: true }
        resource:
          type: object
          required: true
          fields:
            type: { type: string, required: true }
            id: { type: string, required: true }

    labels:
      type: object
      strict: false
      allow_empty_object: true
```

Why this is strong:

- Policies can rely on stable, named domain fields.
- Missing/renamed fields fail at build time.
- Flexible metadata is scoped (`labels`) instead of replacing the contract.

### Handling standards/exceptions safely

`standards.yaml` and `exceptions.yaml` are authoring inputs. If policies need those concepts, compile them into stable domain fields in the contract (for example `controls`, structured `exceptions`, etc.), not opaque pass-through bags.

### Quick checklist

Before adding a field:

1. Will policies depend on this field's shape?
2. Can we write a meaningful policy test against it?
3. Should type drift fail build?
4. Is this a domain concept rather than an implementation detail?
