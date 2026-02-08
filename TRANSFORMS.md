# Built-in transforms

This document defines opactx built-in transforms and their v0.1 semantics.

## Pipeline model

- Transforms run in order from `opactx.yaml`.
- Each transform reads and writes the assembled context object.
- Context paths use dot notation and must start with `context.`.
- Merge behavior is deterministic: object deep merge, arrays replace, primitives replace, later wins.

## Core built-ins (v0.1)

### `mount`

Mount a source payload under a context path.

Input:

- `source_id` (string)
- `target` (string, `context.<path>`)
- optional `strategy`: `merge` (default), `deep`, `replace`

Example:

```yaml
- name: mount
  type: builtin
  with:
    source_id: inventory
    target: context.inventory
```

### `merge`

Merge multiple values with deterministic precedence.

Input:

- `target` (string, `context.<path>`)
- `from` (list): each item is either
  - a reference path (`context.*`, `sources.*`, `intent.*`), or
  - a literal object/value
- optional `include_existing` (bool, default `false`)

Semantics:

- object + object => deep merge
- arrays replace
- primitives replace
- later list entries win

Example:

```yaml
- name: merge
  type: builtin
  with:
    target: context.request
    from:
      - context.defaults
      - context.overrides
```

### `pick`

Select an allowlist of keys from one object.

Input:

- `path` (`context.<path>`)
- `keys` (list of strings)
- optional `target` (`context.<path>`, defaults to `path`)
- optional `strict` (bool, default `false`)

Example:

```yaml
- name: pick
  type: builtin
  with:
    path: context.repo
    keys: [team_id, repo, owners]
```

### `rename`

Move/rename values between paths.

Input:

- single move:
  - `from` (`context.<path>`)
  - `to` (`context.<path>`)
- or multiple moves in `moves`
- optional `ignore_missing` (default `true`)

Example:

```yaml
- name: rename
  type: builtin
  with:
    from: context.repo.team_id
    to: context.repo.team
```

### `coerce`

Normalize values to basic types.

Input:

- `rules` list of:
  - `path` (`context.<path>`)
  - `type`: `bool`, `int`, `float`, `string`, `timestamp`
  - optional `ignore_missing`

Timestamp output is RFC3339 UTC (`Z`). Date-only input like `2026-01-01` becomes `2026-01-01T00:00:00Z`.

Example:

```yaml
- name: coerce
  type: builtin
  with:
    rules:
      - path: context.flags.enabled
        type: bool
      - path: context.limits.count
        type: int
      - path: context.meta.generated_at
        type: timestamp
```

### `defaults`

Set default values only when a field is missing.

Input (supported forms):

- `values`: mapping of `context.<path>` to default value
- or `rules` list with `{ path, value }`
- or single `{ path, value }`

Example:

```yaml
- name: defaults
  type: builtin
  with:
    values:
      context.env: dev
      context.request.region: global
```

### `validate_schema`

Validate the current context object against JSON Schema.

Input:

- optional `schema`: path override
- default schema path comes from `opactx.yaml` (`schema` field)

Notes:

- `opactx build` always runs final schema validation even if this transform is omitted.
- Use this transform when you want an explicit in-pipeline checkpoint.

Example:

```yaml
- name: validate_schema
  type: builtin
  with: {}
```

## Strongly recommended built-ins

### `ref_resolve`

Resolve in-context references from lookup maps (local join, no fetching).

Input:

- `rules` list of:
  - `items`: array path (`context.<array_path>`)
  - `lookup`: map path (`context.<map_path>`)
  - `ref_key`: key in each item
  - `target_key`: key to write resolved object
  - optional `required` (bool)
  - optional `copy` (bool, default `true`)

Example:

```yaml
- name: ref_resolve
  type: builtin
  with:
    rules:
      - items: context.repos
        lookup: context.teams_by_id
        ref_key: team_id
        target_key: team
        required: true
```

### `sort_stable`

Stable sort arrays by a key.

Input:

- `path`: array path (`context.<path>`)
- optional `by`: key in each array item
- optional `order`: `asc` (default) or `desc`

Example:

```yaml
- name: sort_stable
  type: builtin
  with:
    path: context.repos
    by: name
    order: asc
```

### `dedupe`

Remove duplicate array items by key.

Input:

- `path`: array path (`context.<path>`)
- optional `by`: key in each array item (if omitted, item value is used)
- optional `keep`: `first` (default) or `last`

Example:

```yaml
- name: dedupe
  type: builtin
  with:
    path: context.repos
    by: id
    keep: first
```

## Compatibility built-in

### `canonicalize`

Reset context to canonical base shape:

- `standards` from intent
- `exceptions` from intent
- `sources` from fetched source outputs

This exists for backward compatibility and explicit resets in a transform chain.
