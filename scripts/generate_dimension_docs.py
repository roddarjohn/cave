"""Generate RST docs for built-in dimension types.

Imports each example module from ``scripts/examples/``,
creates tables and views on a real PostgreSQL database,
captures real ``psql`` output for schema and query results,
and writes RST files with ERD diagrams to ``docs/_generated/``.

Requires ``DATABASE_URL`` to be set.
"""

import importlib.util
import os
import re
import subprocess
from pathlib import Path
from types import ModuleType

from sqlalchemy import Column, MetaData, create_engine, text

_HERE = Path(__file__).resolve().parent
_EXAMPLES = _HERE / "examples"
_OUT = _HERE.parent / "docs" / "_generated"


# -- Module loading --------------------------------------------------


def _load_example(path: Path) -> ModuleType:
    """Import an example module by file path."""
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        msg = f"cannot load {path}"
        raise ImportError(msg)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# -- RST helpers -----------------------------------------------------


def _directive(
    name: str,
    content: str,
    argument: str = "",
) -> str:
    """Build a Sphinx directive with indented content."""
    arg_suffix = f" {argument}" if argument else ""
    lines = [f".. {name}::{arg_suffix}", ""]
    lines.extend("   " + line for line in content.splitlines())
    return "\n".join(lines)


# -- Table introspection (for ERD only) ------------------------------


def _type_label(col: Column) -> str:
    """Short type name for a SQLAlchemy column."""
    return type(col.type).__name__.upper()


def _table_schema(
    metadata: MetaData,
) -> list[dict]:
    """Extract schema info for every table on *metadata*.

    Returns a list of dicts, each with ``fullname``,
    ``columns`` (list of ``(name, type, flags)`` tuples),
    and ``fk_targets`` (list of target table fullnames).
    """
    result = []
    for key in sorted(metadata.tables):
        tbl = metadata.tables[key]
        cols: list[tuple[str, str, str]] = []
        fk_targets: list[str] = []
        for c in tbl.columns:
            flags: list[str] = []
            if c.primary_key:
                flags.append("PK")
            for fk in c.foreign_keys:
                flags.append("FK")
                parts = fk.target_fullname.rsplit(".", 1)
                fk_targets.append(parts[0])
            if not c.nullable and not c.primary_key:
                flags.append("NOT NULL")
            cols.append((c.name, _type_label(c), " ".join(flags)))
        result.append(
            {
                "fullname": key,
                "columns": cols,
                "fk_targets": fk_targets,
            }
        )
    return result


# -- DOT generation --------------------------------------------------


def _dot_node(
    node_id: str,
    fullname: str,
    columns: list[tuple[str, str, str]],
    *,
    bgcolor: str = "lightblue",
    suffix: str = "",
) -> str:
    """Generate a DOT node with an HTML-label table."""
    rows = []
    for name, typ, flags in columns:
        flag_html = ""
        if flags:
            flag_html = f'  <i><font color="gray40">{flags}</font></i>'
        rows.append(
            f"      <tr>"
            f'<td align="left">{name}</td>'
            f'<td align="left">{typ}'
            f"{flag_html}</td></tr>"
        )
    row_block = "\n".join(rows)
    return (
        f"  {node_id} [label=<\n"
        f'    <table border="1" cellborder="0"'
        f' cellspacing="0" cellpadding="4">\n'
        f'      <tr><td colspan="2"'
        f' bgcolor="{bgcolor}">'
        f"<b>{fullname}{suffix}</b>"
        f"</td></tr>\n"
        f"{row_block}\n"
        f"    </table>\n"
        f"  >];"
    )


def _build_dot(
    tables: list[dict],
    views: list[dict] | None = None,
    extra_edges: list[tuple[str, str, str]] | None = None,
) -> str:
    """Build a complete DOT digraph for an ERD.

    Args:
        tables: From :func:`_table_schema`.
        views: View specs, each a dict with ``fullname`` and
            ``columns`` (same format as table columns).
        extra_edges: ``(source, target, dot_attrs)`` tuples
            for additional edges (e.g. view-to-table).

    """
    views = views or []
    extra_edges = extra_edges or []

    parts = [
        "digraph {",
        "  rankdir=LR;",
        '  node [shape=plaintext fontname="Helvetica" fontsize=11];',
        '  edge [fontname="Helvetica" fontsize=9];',
        "",
    ]

    for t in tables:
        nid = t["fullname"].replace(".", "_")
        parts.append(_dot_node(nid, t["fullname"], t["columns"]))
        parts.append("")

    for v in views:
        nid = v["fullname"].replace(".", "_")
        parts.append(
            _dot_node(
                nid,
                v["fullname"],
                v["columns"],
                bgcolor="lightyellow",
                suffix=" (view)",
            )
        )
        parts.append("")

    # FK edges from table introspection
    for t in tables:
        src = t["fullname"].replace(".", "_")
        for target in t.get("fk_targets", []):
            dst = target.replace(".", "_")
            parts.append(f"  {src} -> {dst};")

    # Extra edges (view -> source, etc.)
    for src, dst, attrs in extra_edges:
        src_id = src.replace(".", "_")
        dst_id = dst.replace(".", "_")
        parts.append(f"  {src_id} -> {dst_id} [{attrs}];")

    parts.append("}")
    return "\n".join(parts)


# -- Database interaction --------------------------------------------


def _create_views(conn, metadata: MetaData) -> None:  # noqa: ANN001
    """CREATE VIEW for each view registered on *metadata*."""
    views_obj = metadata.info.get("views")
    if not views_obj:
        return
    for view in views_obj:
        schema = view.schema or "public"
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        defn = view.compile_definition()
        fqn = f"{schema}.{view.name}"
        conn.execute(text(f"CREATE OR REPLACE VIEW {fqn} AS {defn}"))


def _psql_url(sqlalchemy_url: str) -> str:
    """Convert a SQLAlchemy URL to a plain libpq URL.

    Strips the ``+driver`` portion so psql can connect.
    """
    return re.sub(r"\+\w+", "", sqlalchemy_url)


def _psql(url: str, command: str) -> str:
    """Run a psql command and return its stdout."""
    result = subprocess.run(  # noqa: S603
        ["psql", url, "-c", command],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.rstrip()


# -- RST section builders -------------------------------------------


def _schema_lines(url: str, tables: list[dict]) -> list[str]:
    """RST lines showing ``\\d`` output for each table."""  # noqa: D301
    parts: list[str] = []
    for t in tables:
        output = _psql(url, rf"\d {t['fullname']}")
        block = rf"=# \d {t['fullname']}" + "\n" + output
        parts.append(_directive("code-block", block, argument="text"))
        parts.append("")
    return parts


def _query_lines(
    url: str,
    queries: list[dict],
) -> list[str]:
    """Run each query via psql and build RST code blocks."""
    parts: list[str] = []
    for q in queries:
        sql = q["query"]
        if q.get("description"):
            parts.append(q["description"])
            parts.append("")
        output = _psql(url, sql)
        block = f"=# {sql}\n{output}"
        parts.append(_directive("code-block", block, argument="text"))
        parts.append("")
    return parts


# -- Main ------------------------------------------------------------


def _generate_one(
    slug: str,
    mod: ModuleType,
    conn,  # noqa: ANN001
    url: str,
) -> None:
    """Generate the RST include file for one dimension."""
    tables = _table_schema(mod.metadata)
    dot = _build_dot(tables, mod.VIEWS, mod.EXTRA_EDGES)

    # Create schemas, drop pre-existing tables, then create fresh
    for t in tables:
        schema = t["fullname"].split(".")[0]
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    for key in reversed(list(mod.metadata.tables)):
        conn.execute(text(f"DROP TABLE IF EXISTS {key} CASCADE"))
    mod.metadata.create_all(conn)
    _create_views(conn, mod.metadata)

    # Insert sample data from seed SQL file
    seed_path = _EXAMPLES / mod.SEED_FILE
    seed_sql = seed_path.read_text()
    for raw_stmt in seed_sql.strip().split(";"):
        cleaned = raw_stmt.strip()
        if cleaned:
            conn.execute(text(cleaned))

    # Commit so psql can see the data
    conn.commit()

    psql_url = _psql_url(url)
    parts = [
        "Schema",
        "^^^^^^",
        "",
        mod.SCHEMA_DESCRIPTION,
        "",
        _directive("graphviz", dot),
        "",
        *_schema_lines(psql_url, tables),
        "Sample queries",
        "^^^^^^^^^^^^^^",
        "",
        *_query_lines(psql_url, mod.QUERIES),
    ]
    path = _OUT / f"dim_{slug}.rst"
    path.write_text("\n".join(parts))
    print(f"wrote {path}")

    # Clean up committed tables
    for key in reversed(list(mod.metadata.tables)):
        conn.execute(text(f"DROP TABLE IF EXISTS {key} CASCADE"))
    conn.commit()


_EXAMPLES_LIST = [
    ("simple", "simple.py"),
    ("append_only", "append_only.py"),
    ("eav", "eav.py"),
]


def main() -> None:
    """Write generated RST files to docs/_generated/."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        msg = "DATABASE_URL must be set"
        raise RuntimeError(msg)

    _OUT.mkdir(parents=True, exist_ok=True)
    engine = create_engine(url)

    for slug, filename in _EXAMPLES_LIST:
        mod = _load_example(_EXAMPLES / filename)
        with engine.connect() as conn:
            _generate_one(slug, mod, conn, url)


if __name__ == "__main__":
    main()
