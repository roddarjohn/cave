"""Mako template loading utility."""

from pathlib import Path

from mako.template import Template


def load_template(path: Path) -> Template:
    """Load a Mako template from the given path.

    Args:
        path: Absolute path to the ``.mako`` template file.

    Returns:
        A compiled Mako ``Template``.

    """
    return Template(filename=str(path))  # noqa: S702
