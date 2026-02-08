Contributing to opactx

Thank you for your interest in contributing to opactx.

opactx is infrastructure tooling.
That means correctness, restraint, and clarity matter more than feature velocity.

This document explains how to contribute, what kinds of contributions are welcome, and what will be declined.

Project philosophy (read this first)

opactx is built around a single, explicit idea:

Policy context should be compiled, validated, and versioned before it is evaluated.

Everything in the project exists to support that idea.

If a proposed change:

blurs the build-time boundary,

introduces runtime behavior,

couples context construction to policy semantics,

or expands opactx into a platform,

it is likely out of scope.

What we welcome
1. Bug fixes

Especially:

schema validation issues

determinism bugs

incorrect failure attribution

edge cases in sources or transforms

Bug reports with:

minimal repros

clear expected vs actual behavior

stage-specific failures
are highly appreciated.

2. Source plugins

New source implementations are welcome if they:

fetch data only (no policy logic),

return JSON-serializable output,

are deterministic,

follow the source contract.

Examples:

inventory systems

directory APIs

cloud provider metadata

internal tooling exports

If a source embeds business logic, it will be rejected.

3. Transform improvements

We welcome transforms that:

normalize inconsistent real-world data,

improve canonicalization,

reduce entropy before schema validation.

Transforms must be:

pure (no side effects),

deterministic,

policy-agnostic.

4. Documentation improvements

High-quality documentation is a first-class contribution.

This includes:

clarifying mental models,

improving examples,

documenting failure modes,

tightening language.

Marketing-style documentation will be declined.

5. Tests

Tests are encouraged, especially:

stage lifecycle tests,

schema validation tests,

determinism tests,

event emission tests.

opactx favors behavioral tests over snapshot output tests.

What we do NOT accept

To save everyone time, the following categories are considered out of scope:

❌ Runtime features

background services

long-lived processes

dynamic context updates

caching layers tied to runtime behavior

❌ Policy-aware logic

transforms that inspect or interpret Rego

context shaping based on policy semantics

conditional context generation based on decisions

❌ Interactive workflows

wizards

prompts

step-by-step fixers

REPL-like behavior

❌ UI platforms

dashboards

web interfaces

visual policy editors

These belong in other tools.

Codebase overview

Before contributing, familiarize yourself with:

core/
Context compilation logic, stages, events.

cli/
Command definitions and argument parsing.

renderers/
TTY and non-TTY output renderers.

plugins/
Built-in sources and transforms.

schema/
Schema handling and validation logic.

The core must never depend on renderers.

Architectural invariants (do not break these)

Any contribution must preserve the following invariants:

Build-time only

Explicit schema validation

Canonical data.context namespace

Deterministic output

Stage-based execution

Event-driven core

OPA remains external

If your change violates one of these, it will be rejected.

Development setup

Recommended setup:

python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"


Run tests:

pytest


Run formatting and linting:

ruff check .
ruff format .


(Exact tooling may evolve; check pyproject.toml.)

Writing a good PR

A good pull request should include:

Clear problem statement

Why this belongs in opactx

Which architectural constraints it respects

Tests or justification for not adding tests

Documentation updates if behavior changes

If your PR changes behavior:

update the relevant docs

mention affected stage IDs

describe failure modes

Commit and PR style

Small, focused commits

Descriptive commit messages

Avoid mechanical refactors mixed with behavior changes

Avoid drive-by formatting changes

Issue discussions

Issues are welcome for:

design questions

architectural clarifications

proposal discussions

Please do not open issues asking:

“Can opactx do X instead?”

“Why not make this a service?”
without first understanding the architecture docs.

Plugin contributions

If contributing a plugin:

document the plugin contract

include usage examples

state compatibility expectations

do not assume privileged execution environments

Plugins execute trusted code. This is by design.

Code of conduct

Be respectful and professional.

Disagreements about design are expected; personal attacks are not tolerated.

Final note

opactx values:

clarity over cleverness

restraint over features

long-term correctness over short-term convenience

If you share those values, your contributions are welcome.