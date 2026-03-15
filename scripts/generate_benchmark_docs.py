r"""Generate ``docs/benchmark_results.rst`` from JSON.

Usage::

    uv run pytest tests/benchmarks/ \
        --benchmark-json=docs/_generated/benchmark_results.json
    uv run python scripts/generate_benchmark_docs.py

The generated RST file is ``.. include::``-d from
``docs/benchmarks.rst``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_DOCS_DIR = Path(__file__).resolve().parents[1] / "docs"
_RESULTS_JSON = _DOCS_DIR / "_generated" / "benchmark_results.json"
_OUTPUT_RST = _DOCS_DIR / "benchmark_results.rst"

# Map test file stems to display headings and ordinals.
_SECTIONS: dict[str, tuple[int, str]] = {
    "test_simple_bench": (0, "Simple dimension"),
    "test_append_only_bench": (1, "Append-only dimension"),
    "test_eav_bench": (2, "EAV dimension"),
    "test_ledger_bench": (3, "Ledger"),
}

# Friendly benchmark descriptions keyed by test name.
_DESCRIPTIONS: dict[str, str] = {
    "test_insert_single": "Insert one row into the backing table.",
    "test_update_single": "Update one row in the backing table.",
    "test_delete_single": ("Delete one row (includes re-insert each round)."),
    "test_insert_batch_100": "Insert 100 rows in a single statement.",
    "test_insert_batch_1000": ("Insert 1,000 rows in a single statement."),
    "test_select_all": "SELECT * from the view (10k rows seeded).",
    "test_select_filtered": ("SELECT with WHERE clause (10k rows seeded)."),
    "test_select_latest": ("SELECT * from the join view (10k entities)."),
    "test_select_after_many_updates": (
        "SELECT after 100 revisions of one entity."
    ),
    "test_update_single_attribute": (
        "Update one attribute (appends a new row)."
    ),
    "test_select_pivot": ("SELECT * from the pivot view (10k entities)."),
    "test_select_pivot_filtered": ("Filtered pivot view query (10k entities)."),
    "test_select_balance": ("SUM(value) GROUP BY category (10k entries)."),
}

# Test names whose mean populates the comparison "read" column.
_READ_BENCHMARKS = {
    "test_select_all",
    "test_select_latest",
    "test_select_pivot",
    "test_select_balance",
}


def _fmt(val: float) -> str:
    """Format a microsecond value with comma separators."""
    return f"{val:,.2f}"


def _section_key(bench: dict) -> tuple[int, str]:
    """Return ``(order, heading)`` for a benchmark entry."""
    parts = bench["fullname"].split("::")
    stem = parts[0].rsplit("/", 1)[-1].replace(".py", "")
    order, heading = _SECTIONS.get(stem, (99, stem))
    return (order, heading)


def _group_benchmarks(
    benchmarks: list[dict],
) -> tuple[dict[str, list[dict]], list[str]]:
    """Group benchmarks by section and return sorted headings."""
    sections: dict[str, list[dict]] = {}
    order_map: dict[str, int] = {}
    for bench in benchmarks:
        order, heading = _section_key(bench)
        sections.setdefault(heading, []).append(bench)
        order_map.setdefault(heading, order)
    sorted_headings = sorted(sections, key=lambda h: order_map[h])
    return sections, sorted_headings


def _render_section(
    heading: str,
    benches: list[dict],
    comparison: dict[str, dict[str, float]],
) -> list[str]:
    """Render one dimension section as RST lines."""
    lines: list[str] = []
    lines.append(heading)
    lines.append("~" * len(heading))
    lines.append("")

    for bench in benches:
        desc = _DESCRIPTIONS.get(bench["name"], "")
        if desc:
            lines.append(f"- ``{bench['name']}`` — {desc}")
    lines.append("")

    lines.extend(
        [
            ".. list-table::",
            "   :header-rows: 1",
            "   :widths: 40 15 15 15 15",
            "",
            "   * - Benchmark",
            "     - Min (us)",
            "     - Mean (us)",
            "     - Median (us)",
            "     - Rounds",
        ]
    )

    for bench in benches:
        name = bench["name"]
        stats = bench["stats"]
        min_us = stats["min"] * 1_000_000
        mean_us = stats["mean"] * 1_000_000
        median_us = stats["median"] * 1_000_000

        lines.append(f"   * - ``{name}``")
        lines.append(f"     - {_fmt(min_us)}")
        lines.append(f"     - {_fmt(mean_us)}")
        lines.append(f"     - {_fmt(median_us)}")
        lines.append(f"     - {stats['rounds']:,}")

        if name == "test_insert_single":
            comparison[heading] = {
                "insert_mean": mean_us,
                "insert_median": median_us,
            }
        if name in _READ_BENCHMARKS:
            comparison.setdefault(heading, {})["select_mean"] = mean_us

    lines.append("")
    return lines


def _render_comparison(
    sorted_headings: list[str],
    comparison: dict[str, dict[str, float]],
) -> list[str]:
    """Render the cross-dimension comparison table."""
    lines: list[str] = [
        "Cross-dimension comparison",
        "~~~~~~~~~~~~~~~~~~~~~~~~~~",
        "",
        "Single-row insert and primary read query across dimension types:",
        "",
        ".. list-table::",
        "   :header-rows: 1",
        "   :widths: 30 20 20 20",
        "",
        "   * - Dimension type",
        "     - Insert mean (us)",
        "     - Insert median (us)",
        "     - Read mean (us)",
    ]
    for heading in sorted_headings:
        vals = comparison.get(heading, {})
        im = vals.get("insert_mean")
        imed = vals.get("insert_median")
        sm = vals.get("select_mean")
        lines.append(f"   * - {heading}")
        lines.append(f"     - {_fmt(im) if im else 'n/a'}")
        lines.append(f"     - {_fmt(imed) if imed else 'n/a'}")
        lines.append(f"     - {_fmt(sm) if sm else 'n/a'}")
    lines.append("")
    return lines


def main() -> None:
    """Read benchmark JSON and write RST results tables."""
    if not _RESULTS_JSON.exists():
        print(
            f"No results file at {_RESULTS_JSON}.\nRun: just bench-docs",
            file=sys.stderr,
        )
        sys.exit(1)

    data = json.loads(_RESULTS_JSON.read_text())
    sections, sorted_headings = _group_benchmarks(data["benchmarks"])

    lines: list[str] = [
        ".. This file is auto-generated by scripts/generate_benchmark_docs.py",
        "",
    ]
    comparison: dict[str, dict[str, float]] = {}
    for heading in sorted_headings:
        lines.extend(_render_section(heading, sections[heading], comparison))
    lines.extend(_render_comparison(sorted_headings, comparison))

    _OUTPUT_RST.write_text("\n".join(lines))
    print(f"wrote {_OUTPUT_RST}")


if __name__ == "__main__":
    main()
