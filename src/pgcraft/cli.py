"""pgcraft CLI entrypoints."""

from __future__ import annotations

import sys
import traceback
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy import create_engine

from pgcraft.alembic.register import pgcraft_alembic_hook
from pgcraft.runtime.apply import apply_ops
from pgcraft.runtime.builder import build_metadata
from pgcraft.runtime.config import DimensionConfig
from pgcraft.runtime.filter import filter_safe_ops
from pgcraft.runtime.generate import generate_ops
from pgcraft.runtime.registry import DimensionRegistry, ensure_registry_table

app = typer.Typer(help="pgcraft — configuration-driven PostgreSQL framework.")


def _load_config(config_path: str) -> DimensionConfig:
    """Read and validate a DimensionConfig from *config_path*.

    Args:
        config_path: Filesystem path to a JSON file containing a
            serialised :class:`~pgcraft.runtime.config.DimensionConfig`.

    Returns:
        The validated :class:`~pgcraft.runtime.config.DimensionConfig`.

    Raises:
        SystemExit: If the file cannot be read or the JSON is invalid.

    """
    try:
        raw = Path(config_path).read_text()
        return DimensionConfig.model_validate_json(raw)
    except Exception as exc:
        typer.echo(f"Error loading config: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _apply(
    conn: object,
    config: DimensionConfig,
    schema: str,
    entry: DimensionRegistry,
) -> None:
    """Run the full generate → filter → apply pipeline for *entry*.

    Args:
        conn: An active SQLAlchemy connection.
        config: The validated dimension configuration.
        schema: The target PostgreSQL schema.
        entry: The registry entry to update.

    Raises:
        Any exception raised during generation, filtering, or application.

    """
    from sqlalchemy import Connection as _Conn  # noqa: PLC0415 (local for type)

    assert isinstance(conn, _Conn)  # noqa: S101

    entry.mark_applying(conn)
    ops = generate_ops(conn, build_metadata(config, schema=schema), schema)
    safe_ops = filter_safe_ops(ops)
    executed_sql = apply_ops(conn, safe_ops)
    entry.mark_done(conn, sql=executed_sql)
    typer.echo(
        f"Applied {len(safe_ops)} op(s) to {schema!r}. "
        f"Registry entry id={entry.id}."
    )


@app.command("generate-schema")
def generate_schema(
    config_path: Annotated[
        str,
        typer.Argument(help="Path to a DimensionConfig JSON file."),
    ],
    schema: Annotated[
        str,
        typer.Option(
            "--schema",
            "-s",
            help="Target PostgreSQL schema (tenant name).",
        ),
    ],
    database_url: Annotated[
        str,
        typer.Option(
            "--database-url",
            envvar="DATABASE_URL",
            help="SQLAlchemy database URL.",
        ),
    ],
    dry_run: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            "--dry-run",
            help="Print the ops without touching the database.",
        ),
    ] = False,
) -> None:
    r"""Apply a DimensionConfig JSON file to a tenant schema.

    Reads *config_path*, builds the desired SQLAlchemy MetaData, diffs it
    against the live database restricted to *schema*, filters the resulting
    ops through the safety allowlist, and applies them inside a transaction.

    Every attempt is recorded in ``pgcraft.dimension_registry`` for
    auditability.  Use ``--dry-run`` to inspect the op list without writing
    anything to the database.

    Example::

        pgcraft generate-schema product.json --schema tenant_abc \
            --database-url postgresql+psycopg://localhost/mydb

    """
    config = _load_config(config_path)

    pgcraft_alembic_hook()

    engine = create_engine(database_url)

    with engine.connect() as conn:
        with conn.begin():
            ensure_registry_table(conn)

        if dry_run:
            with conn.begin():
                metadata = build_metadata(config, schema=schema)
                ops = generate_ops(conn, metadata, schema)
                safe_ops = filter_safe_ops(ops)
            typer.echo(
                f"Dry run — {len(safe_ops)} op(s) would be applied to "
                f"{schema!r}:"
            )
            for op in safe_ops:
                typer.echo(f"  {type(op).__name__}")
            return

        with conn.begin():
            entry = DimensionRegistry.create(conn, config=config, schema=schema)

        try:
            with conn.begin():
                _apply(conn, config, schema, entry)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            try:
                with conn.begin():
                    entry.mark_error(conn, message=message)
            except Exception:  # noqa: BLE001, S110
                pass
            typer.echo(f"Error: {message}", err=True)
            if "--debug" in sys.argv:
                traceback.print_exc()
            raise typer.Exit(code=1) from exc
