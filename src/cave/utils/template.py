"""Mako template loading for dimension factories."""

from pathlib import Path

from mako.template import Template

_TEMPLATE_DIR = (
    Path(__file__).resolve().parent.parent
    / "factory"
    / "dimension"
    / "templates"
)


def load_template(name: str) -> Template:
    """Load a Mako template from the templates directory."""
    return Template(filename=str(_TEMPLATE_DIR / name))  # noqa: S702
