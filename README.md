# opactx

opactx is a contract-first context compiler for Open Policy Agent (OPA).

It assembles human-owned intent and external system data into a deterministic, schema-validated
context bundle that OPA can consume safely and consistently.

Policies should depend on stable data.
opactx makes unstable data conform to a stable contract.

## Why opactx exists

OPA evaluates policies against input and data — but it does not:

- validate the shape of data
- protect against schema drift
- manage external context sources
- enforce determinism or auditability

In real systems, policy context comes from:

- YAML standards
- exception lists
- APIs
- CLIs
- inventories
- directories

That data is messy, changes over time, and breaks policies silently.

opactx solves this by acting as a compiler.

It:

- gathers context from multiple sources
- normalizes it into a canonical shape
- validates it against an explicit schema
- emits a deterministic OPA bundle

OPA only ever sees clean, shaped input.

## Mental model

Think of opactx as:

gcc for policy context

YAML + APIs + scripts → context  
JSON Schema → ABI  
Bundle → binary  
OPA → runtime

## What opactx is (and is not)

opactx is:

- a build-time tool
- deterministic
- schema-first
- audit-friendly
- OPA-native (bundles, manifests)

opactx is not:

- a policy engine
- an OPA replacement
- a runtime service
- a data lake
- a workflow orchestrator

## Core concepts

### 1) Context contract (context.schema.json)

Defines the exact shape of data.context.

If the compiled context doesn’t match the schema, the build fails.

This prevents:

- silent policy breakage
- undefined behavior in Rego
- production surprises

### 2) Intent (context/*.yaml)

Human-authored, reviewable files:

- standards.yaml
- exceptions.yaml

These describe what should be true, not how to fetch it.

### 3) Sources (connectors)

Declarative connectors defined in opactx.yaml:

- file
- http
- exec
- plugins via entry points

Sources fetch reality.

### 4) Transforms

Transforms normalize raw source data into the canonical contract.

Policies never depend on raw source output.

### 5) Bundle output

opactx build produces an OPA bundle:

```
dist/bundle/
├─ data.json      # { "context": ... }
└─ .manifest      # revision + roots
```

This bundle is what you deploy.

## Installation

```
pip install opactx
```

or (recommended for isolation):

```
pipx install opactx
```

## Quick start

Initialize a project:

```
opactx init --with-examples
```

Validate config and intent (fast, no network):

```
opactx validate --strict
```

Build the context bundle:

```
opactx build
```

Inspect the output:

```
opactx inspect dist/bundle
```

## Example policy usage

With opactx, policies depend on a stable namespace:

```
data.context.standards
data.context.exceptions
data.context.sources
```

They never depend on raw APIs or ad-hoc data layouts.

This keeps Rego:

- simple
- testable
- portable

## Development workflow (dev → prod)

Develop

- edit context/*.yaml
- adjust sources/transforms
- run opactx validate

Build

- opactx build
- generate deterministic bundle

Test

- run OPA / conftest against the bundle

CI

- validate + build
- publish bundle artifact

Deploy

- promote the same bundle to staging and prod

## Commands

| Command | Purpose |
| --- | --- |
| opactx init | Scaffold a new project |
| opactx validate | Fast preflight checks (no source fetching) |
| opactx build | Compile context into an OPA bundle |
| opactx inspect | Inspect bundles and context |
| opactx run-opa | Dev-only wrapper to run OPA with a bundle |

## Design principles

Contract first  
Policies depend on schema, not data shape accidents.

Fail early  
Invalid context fails at build time, not in production.

Deterministic  
Same inputs → same bundle bytes.

Separation of concerns  
opactx builds context; OPA evaluates policy.

Auditability  
Every bundle has a revision you can trace.

## Status

⚠️ Early-stage / experimental

APIs may evolve  
schemas should be versioned carefully  
feedback welcome

## Contributing

Contributions are welcome, especially:

- source plugins
- transform patterns
- real-world schemas
- test cases

Please read CONTRIBUTING.md before opening a PR.
