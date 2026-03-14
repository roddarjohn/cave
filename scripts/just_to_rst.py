"""Convert `just --list` output to RST definition list format."""

import re
import sys
from pathlib import Path

from mako.template import Template

TEMPLATE = Template(  # noqa: S702
    filename=str(Path(__file__).parent / "just_to_rst.rst.mako")
)


def _format_args(args: list[str]) -> str:
    if not args:
        return ""
    return " " + " ".join(f"<{a}>" for a in args)


def parse_just_list(text: str) -> list[tuple[str, str, str]]:
    """Parse just --list output into (command, args, description) tuples."""
    entries = []
    for line in text.splitlines():
        match = re.match(r"^\s{4}(\S+)((?:\s+\S+)*?)\s+#\s+(.+)$", line)
        if not match:
            continue
        command, raw_args, description = match.groups()
        entries.append((command, _format_args(raw_args.split()), description))
    return entries


def just_to_rst(text: str) -> str:
    """Convert just --list output to an RST definition list."""
    return TEMPLATE.render(entries=parse_just_list(text))


if __name__ == "__main__":
    sys.stdout.write(just_to_rst(sys.stdin.read()))
