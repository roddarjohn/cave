project = "pgcraft"
author = "Rodda John"
extensions = [
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
html_theme = "furo"
html_title = "pgcraft"
exclude_patterns = ["_generated"]
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
