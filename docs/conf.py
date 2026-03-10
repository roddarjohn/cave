project = "cave"
author = "Rodda John"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
]
html_theme = "furo"
html_title = "cave"
exclude_patterns = ["_generated"]
add_module_names = False
nitpicky = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pydantic": ("https://docs.pydantic.dev/latest", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20", None),
    "typer": ("https://typer.tiangolo.com", None),
}
