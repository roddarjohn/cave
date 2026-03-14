"""Sqlfluff Mako templater plugin.

Renders Mako templates with placeholder variables so that sqlfluff
can parse and lint ``.sql.mako`` files.  Control flow (``% if``,
``% for``) is evaluated by Mako itself, producing a single valid
SQL branch.

For plain ``.sql`` files the source passes through unchanged.
"""

import logging
import re
from io import StringIO
from typing import Any, Self

from mako.runtime import UNDEFINED, Context
from mako.template import Template
from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.templaters.base import (
    RawFileSlice,
    RawTemplater,
    SQLTemplaterError,
    TemplatedFile,
    TemplatedFileSlice,
)

logger = logging.getLogger(__name__)

# Post-processing: ``AS (__mako_N__)`` -> ``AS (SELECT 1)`` so
# that CTE subquery positions parse as valid SQL.
_CTE_BODY_RE = re.compile(r"\bAS\s*\(__mako_\d+__\)")


class _Placeholder(str):
    """A ``str`` subclass that acts as a SQL-safe placeholder.

    Being a real ``str`` means ``', '.join(placeholders)``
    and f-string interpolation work naturally inside
    ``<% %>`` blocks.
    """

    __slots__ = ("_name",)

    _counter = 0

    def __new__(cls, name: str = "") -> Self:
        cls._counter += 1
        obj = super().__new__(cls, f"__mako_{cls._counter}__")
        obj._name = name
        return obj

    def __iter__(self):  # noqa: ANN204
        """Yield a single placeholder for ``% for`` loops."""
        yield _Placeholder(f"iter_{self._name}")  # type: ignore[unresolved-attribute]

    def __bool__(self) -> bool:
        """Truthy so ``% if var:`` takes the first branch."""
        return True

    def __eq__(self, other: object) -> bool:
        return False

    def __ne__(self, other: object) -> bool:
        return True

    def __hash__(self) -> int:
        return id(self)


class _PlaceholderContext(Context):
    """Mako context returning placeholders for undefined vars."""

    def get(self, key: str, default: Any = None) -> Any:  # noqa: ANN401
        """Return a placeholder for undefined template variables."""
        val = super().get(key, default)
        if val is UNDEFINED:
            return _Placeholder(key)
        return val


def _render_with_placeholders(source: str) -> str:
    """Render a Mako template replacing variables with placeholders."""
    _Placeholder._counter = 0  # noqa: SLF001
    tmpl = Template(text=source, strict_undefined=False)  # noqa: S702
    buf = StringIO()
    ctx = _PlaceholderContext(buf)
    ctx._set_with_template(tmpl)  # noqa: SLF001
    tmpl.render_context(ctx)
    result = buf.getvalue()
    # Fix CTE subquery positions: AS (__mako_N__) -> AS (SELECT 1).
    return _CTE_BODY_RE.sub("AS (SELECT 1)", result)


class MakoTemplater(RawTemplater):
    """Sqlfluff templater that handles Mako syntax in SQL files."""

    name = "mako"

    def process(
        self,
        *,
        in_str: str,
        fname: str,
        config: Any = None,  # noqa: ANN401, ARG002
        formatter: Any = None,  # noqa: ANN401, ARG002
    ) -> tuple[TemplatedFile, list[SQLTemplaterError]]:
        """Process a Mako-templated SQL file."""
        # Plain .sql files have no mako syntax; pass through.
        if "${" not in in_str and "<%" not in in_str and "\n% " not in in_str:
            return TemplatedFile(in_str, fname=fname), []

        try:
            templated_str = _render_with_placeholders(in_str)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Mako rendering failed for %s; falling back to raw",
                fname,
                exc_info=True,
            )
            return TemplatedFile(in_str, fname=fname), []

        if templated_str == in_str:
            return TemplatedFile(in_str, fname=fname), []

        return (
            TemplatedFile(
                source_str=in_str,
                templated_str=templated_str,
                fname=fname,
                sliced_file=[
                    TemplatedFileSlice(
                        "templated",
                        slice(0, len(in_str)),
                        slice(0, len(templated_str)),
                    ),
                ],
                raw_sliced=[
                    RawFileSlice(
                        in_str,
                        "templated",
                        0,
                    ),
                ],
            ),
            [],
        )


@hookimpl  # type: ignore[misc]
def get_templaters() -> list[type[RawTemplater]]:
    """Register the Mako templater with sqlfluff."""
    return [MakoTemplater]
