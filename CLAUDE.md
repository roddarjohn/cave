# CLAUDE.md

## What is cave?

A configuration-driven PostgreSQL dimension/data warehouse framework.
Generates SQLAlchemy models, Alembic migrations, and PostgREST APIs from
declarative dimension configurations. See `PLAN.md` for design philosophy.

## Tooling

This project uses [uv](https://docs.astral.sh/uv/) for dependency management
and virtual environments. Always use `uv run` to execute commands (not raw
`python` or `pip`). Install all dependency groups with `uv sync --all-groups`.
Add dependencies via `uv add`, not `pip install`.

## Commands

```bash
just lint          # ruff check + format check
just type-check    # ty check src/
just dev-test      # pytest (fast, local dev)
just test          # tox (full isolation, matches CI)
just docs          # build Sphinx HTML docs
just setup         # install pre-commit hooks
```

Auto-fix: `uv run --group lint ruff check --fix && uv run --group lint ruff format`

Tests require a running PostgreSQL instance with `DATABASE_URL` set
(e.g. `postgresql+psycopg://postgres@localhost/cave`).

## Code standards

### Guiding principles

- **Postgres is king.** Push logic into the database when it is obviously
  correct and declarative. Python orchestrates; Postgres enforces.
- **Explicit over implicit.** No magic. If behavior is not obvious from
  reading the code, it needs a docstring or comment explaining *why*.
- **Simple over clever.** Prefer the straightforward approach. Three similar
  lines are better than a premature abstraction. Do not design for
  hypothetical future requirements.
- **Readability counts.** Code is read far more than it is written. Optimize
  for the next person reading, not the person writing.
- **Errors should never pass silently.** Catch specific exceptions. Never use
  bare `except:`. Validate at system boundaries (user input, external APIs).
  Trust internal code.

### Style (enforced by ruff + ty)

- **Line length:** 80 characters.
- **Type annotations:** Required on all public API. Avoid `Any` unless
  absolutely necessary. Run `just type-check` to verify.
- **Docstrings:** Google style. Required on all public classes, functions, and
  methods. Describe *what* and *why*, not *how*.
- **Imports:** Top-level only. No local/deferred imports unless there is a
  genuine circular-import reason. Ruff handles ordering.
- **Naming:** `snake_case` for functions/variables, `PascalCase` for classes,
  `UPPER_CASE` for constants. Use descriptive names -- `requires_python` not
  `rp`.

### Testing

- Prefer **integration tests** that hit a real PostgreSQL database over unit
  tests with mocks. Mocks mask migration and schema bugs.
- All changes MUST be tested. If a behavior changed, a test should cover it.
- Use `pytest-alembic` for migration testing.
- Run `just dev-test` for fast feedback, `just test` before submitting.

### Contribution checklist

Before submitting, all of these must pass (CI will enforce):

1. `just lint` -- no lint or format violations
2. `just type-check` -- no type errors
3. `just test` -- full test suite green
4. No hardcoded credentials, secrets, or debug statements

## Architecture notes

- `src/cave/factory/` -- Dimension table factories (simple, append-only, EAV).
  Configuration in, SQLAlchemy models out.
- `src/cave/alembic/` -- Custom Alembic integration: schema discovery,
  sqlglot-based SQL formatting in migrations, role/grant handling.
- `src/cave/models/` -- PostgREST role and grant declarations.
- `src/cave/resource.py` -- PostgREST API resource registration.
- `playground/` -- Example project showing end-to-end usage.

## Hooks

A post-tool-use hook runs `just lint && just type-check` after every file
edit. If the hook fails, fix the issue before continuing.
