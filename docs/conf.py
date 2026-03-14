import tomllib
from pathlib import Path

_pyproject = tomllib.loads(
    (Path(__file__).resolve().parent.parent / "pyproject.toml")
    .read_text()
)

project = "pgcraft"
author = "Rodda John"
version = _pyproject["project"]["version"]
release = version
extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.graphviz",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
]
graphviz_output_format = "svg"

# -- Theme -----------------------------------------------------------
html_theme = "alabaster"
html_title = "pgcraft"
html_theme_options = {
    "description": "Configuration-driven PostgreSQL framework",
    "github_user": "roddajohn",
    "github_repo": "pgcraft",
    "github_button": True,
    "github_type": "star",
    "fixed_sidebar": True,
    "show_powered_by": False,
    "sidebar_collapse": True,
    "extra_nav_links": {},
    "font_family": (
        "'Source Sans Pro', 'Segoe UI', Helvetica, Arial,"
        " sans-serif"
    ),
    "code_font_family": (
        "'Source Code Pro', 'SFMono-Regular', Menlo,"
        " Consolas, monospace"
    ),
    "page_width": "940px",
    "sidebar_width": "220px",
}
html_sidebars = {
    "**": [
        "about.html",
        "searchbox.html",
        "navigation.html",
        "relations.html",
        "versioning.html",
    ],
}

# -- General ---------------------------------------------------------
exclude_patterns = ["_generated"]
myst_heading_anchors = 3
add_module_names = False
nitpicky = True
nitpick_ignore = [
    # alembic does not publish an intersphinx inventory
    ("py:class", "alembic.operations.ops.MigrateOperation"),
    ("py:class", "alembic.autogenerate.rewriter.Rewriter"),
    # private Protocol used only for internal type checking
    ("py:class", "pgcraft.validator._SchemaItemValidator"),
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pydantic": ("https://docs.pydantic.dev/latest", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20", None),
    "typer": ("https://typer.tiangolo.com", None),
}

templates_path = ["_templates"]
