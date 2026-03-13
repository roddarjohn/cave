"""Generate RST docs for built-in dimension types.

Imports each example module from ``scripts/examples/``,
introspects the generated MetaData, and writes RST files
with ERD diagrams, schema tables, and sample data to
``docs/_generated/``.
"""

import importlib.util
from pathlib import Path
from types import ModuleType

from sqlalchemy import Column, MetaData

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


def _psql_table(
    headers: list[str],
    rows: list[list[str]],
) -> str:
    """Format *headers* and *rows* as psql-style output.

    Returns a plain-text table like::

       id | name  | email
      ----+-------+-------------------
        1 | Alice | alice@example.com
        2 | Bob   | bob@example.com
    """
    all_rows = [headers, *rows]
    widths = [max(len(row[i]) for row in all_rows) for i in range(len(headers))]

    def _fmt(row: list[str], pad: str = " ") -> str:
        cells = [f"{pad}{cell:<{widths[i]}}{pad}" for i, cell in enumerate(row)]
        return "|".join(cells)

    header_line = _fmt(headers)
    sep = "+".join("-" * (w + 2) for w in widths)
    data = "\n".join(_fmt(r) for r in rows)
    return f"{header_line}\n{sep}\n{data}"


# -- Table introspection ---------------------------------------------


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


# -- RST section builders -------------------------------------------


def _schema_lines(tables: list[dict]) -> list[str]:
    """RST lines for schema tables (no heading)."""
    parts: list[str] = []
    for t in tables:
        parts.append(f"``{t['fullname']}``")
        parts.append("")
        parts.append(
            _directive(
                "code-block",
                _psql_table(
                    ["Column", "Type", "Constraints"],
                    list(t["columns"]),
                ),
                argument="text",
            )
        )
        parts.append("")
    return parts


def _sample_lines(samples: list[dict]) -> list[str]:
    """RST lines for sample data as psql-style tables.

    Each *sample* has ``title``, ``headers``, ``rows``,
    and an optional ``description``.
    """
    parts: list[str] = []
    for s in samples:
        parts.append(f"*{s['title']}*")
        if s.get("description"):
            parts.append("")
            parts.append(s["description"])
        parts.append("")
        parts.append(
            _directive(
                "code-block",
                _psql_table(s["headers"], s["rows"]),
                argument="text",
            )
        )
        parts.append("")
    return parts


# -- Main ------------------------------------------------------------


def _generate_one(slug: str, mod: ModuleType) -> None:
    """Generate the RST include file for one dimension."""
    tables = _table_schema(mod.metadata)
    dot = _build_dot(tables, mod.VIEWS, mod.EXTRA_EDGES)

    parts = [
        _directive("graphviz", dot),
        "",
        *_schema_lines(tables),
        *_sample_lines(mod.SAMPLES),
    ]
    path = _OUT / f"dim_{slug}.rst"
    path.write_text("\n".join(parts))
    print(f"wrote {path}")


_EXAMPLES_LIST = [
    ("simple", "simple.py"),
    ("append_only", "append_only.py"),
    ("eav", "eav.py"),
]


def main() -> None:
    """Write generated RST files to docs/_generated/."""
    _OUT.mkdir(parents=True, exist_ok=True)
    for slug, filename in _EXAMPLES_LIST:
        mod = _load_example(_EXAMPLES / filename)
        _generate_one(slug, mod)


if __name__ == "__main__":
    main()
