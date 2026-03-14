"""Sqlfluff Mako templater plugin.

Replaces Mako template syntax with SQL-safe placeholders so that
sqlfluff can parse and lint ``.mako`` files containing SQL.

Mako constructs handled:

- ``${variable}`` expressions -> replaced with placeholder identifiers
- ``<%...%>`` Python blocks -> stripped (replaced with blank lines)
- ``% for ...`` / ``% if ...`` / ``% endif`` / ``% endfor`` control
  flow lines -> replaced with blank lines

Line counts are preserved so that violation line numbers map back
to the original source.
"""

import re
from typing import Any

from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.templaters.base import (
    RawTemplater,
    SQLTemplaterError,
    TemplatedFile,
)

_EXPR_RE = re.compile(r"\$\{[^}]+\}")
_BLOCK_RE = re.compile(r"<%.*?%>\\?\n?", re.DOTALL)
_CONTROL_RE = re.compile(r"^(\s*%\s*(for|if|endif|endfor|else|elif)\b.*)$")

# Counter for generating unique placeholder identifiers.
_placeholder_counter = 0


def _next_placeholder() -> str:
    """Return a unique SQL-safe placeholder identifier."""
    global _placeholder_counter  # noqa: PLW0603
    _placeholder_counter += 1
    return f"__mako_{_placeholder_counter}__"


def _strip_mako(raw_str: str) -> str:
    """Replace Mako syntax with SQL-safe placeholders."""
    global _placeholder_counter  # noqa: PLW0603
    _placeholder_counter = 0

    # Strip <% ... %> blocks, preserving line count.
    def _replace_block(m: re.Match[str]) -> str:
        return "\n" * m.group(0).count("\n")

    result = _BLOCK_RE.sub(_replace_block, raw_str)

    # Replace ${...} expressions with placeholder identifiers.
    result = _EXPR_RE.sub(lambda _: _next_placeholder(), result)

    # Replace % control-flow lines with blank lines.
    lines = result.split("\n")
    out: list[str] = []
    for line in lines:
        if _CONTROL_RE.match(line):
            out.append("")
        else:
            out.append(line)

    return "\n".join(out)


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
        templated_str = _strip_mako(in_str)
        return (
            TemplatedFile(
                source_str=in_str,
                templated_str=templated_str,
                fname=fname,
            ),
            [],
        )


@hookimpl  # type: ignore[misc]
def get_templaters() -> list[type[RawTemplater]]:
    """Register the Mako templater with sqlfluff."""
    return [MakoTemplater]
