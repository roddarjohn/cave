# cave *(working name)*

**Configuration-driven PostgreSQL dimension tables, migrations, and APIs.**

Cave generates SQLAlchemy models, Alembic migrations, and
[PostgREST](https://postgrest.org) API views from declarative dimension
configurations. Define your schema once; cave handles the rest.

> **Note:** "cave" is a working name and may change before the first stable
> release.

---

## Features

- **Dimension types** — Simple, Append-Only (SCD Type 2), and
  Entity-Attribute-Value, each with generated backing tables, views, and
  triggers.
- **Plugin architecture** — Composable plugins with `@requires` / `@produces`
  decorators, executed in topological order.
- **Automatic migrations** — Alembic integration with SQL-formatted migration
  scripts via sqlglot.
- **PostgREST API layer** — API views, roles, and grants declared alongside
  your models.
- **Postgres is king** — Logic lives in the database (views, triggers, check
  constraints). Python orchestrates; Postgres enforces.

## Quick start

### Install

```bash
pip install cave            # or: uv add cave
```

### Define a dimension

```python
from sqlalchemy import Column, MetaData, String
from cave.factory.dimension import SimpleDimensionResourceFactory

metadata = MetaData()

SimpleDimensionResourceFactory(
    tablename="users",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("name", String, nullable=False),
        Column("email", String),
    ],
)
```

This creates:

- A `public.users` table with `id`, `name`, and `email` columns.
- An `api.users` view exposing the same columns for PostgREST.
- Alembic migration support out of the box.

### Generate migrations

```bash
cave migrate revision --autogenerate -m "add users"
cave migrate upgrade head
```

## Dimension types

Cave ships with three built-in dimension types:

| Type | Use case | History? |
|------|----------|----------|
| **Simple** | Static reference data | No |
| **Append-Only** (SCD Type 2) | Track changes over time | Yes |
| **EAV** | Flexible / sparse attributes with full audit trail | Yes |

See the [dimensions documentation](https://cave.readthedocs.io/en/latest/dimensions.html) for ERD diagrams, schema details, and worked examples.

## Documentation

Full documentation is available at
[cave.readthedocs.io](https://cave.readthedocs.io/en/latest/).

- [Setup & installation](https://cave.readthedocs.io/en/latest/setup.html)
- [Built-in dimensions](https://cave.readthedocs.io/en/latest/dimensions.html)
- [Plugin system](https://cave.readthedocs.io/en/latest/plugins.html)
- [API reference](https://cave.readthedocs.io/en/latest/api.html)
- [Development guide](https://cave.readthedocs.io/en/latest/development.html)
- [Playground](https://cave.readthedocs.io/en/latest/playground.html)

## Development

```bash
# Clone and install
git clone https://github.com/yourusername/cave.git
cd cave
uv sync --all-groups

# Run checks
just lint          # ruff check + format
just type-check    # ty check src/
just dev-test      # pytest (fast, needs DATABASE_URL)
just test          # tox (full isolation)
just docs          # build Sphinx HTML docs
```

Tests require a running PostgreSQL instance:

```bash
DATABASE_URL=postgresql+psycopg:///cave just dev-test
```

## Design philosophy

See [PLAN.md](PLAN.md) for the full design philosophy. The short version:

- **Postgres is king.** Push logic into the database when it is obviously
  correct and declarative.
- **Explicit over implicit.** No magic — if behavior is not obvious from
  reading the code, it needs a docstring explaining *why*.
- **Simple over clever.** Three similar lines are better than a premature
  abstraction.

## License

See [LICENSE](LICENSE) for details.
