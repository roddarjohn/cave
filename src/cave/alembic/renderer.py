"""cave's SQL formatting for alembic_utils migration rendering.

alembic_utils embeds view/function definitions as a single ``repr()``-escaped
string, producing one very long line.  This module registers cave's own
renderers for the four alembic_utils op types using alembic's
``renderers.dispatch_for`` extension point, replacing the definition string
with a sqlglot pretty-printed triple-quoted block.

``apply()`` must be called after alembic_utils has been imported (and has
registered its own renderers) so that cave's registrations take precedence.
``cave_alembic_hook()`` handles this ordering.
"""

import textwrap
from typing import TYPE_CHECKING

import sqlglot
from alembic.autogenerate import renderers
from alembic_utils.exceptions import UnreachableException
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.reversible_op import CreateOp, DropOp, ReplaceOp, RevertOp

if TYPE_CHECKING:
    from alembic.autogenerate.api import AutogenContext


def _render_entity_code(target: ReplaceableEntity) -> str:
    """Render the Python variable assignment for a replaceable entity.

    The SQL definition is pretty-printed by sqlglot and embedded as a
    triple-quoted string rather than a single ``repr()``-escaped line.
    """
    var_name = target.to_variable_name()
    class_name = target.__class__.__name__
    formatted_sql = sqlglot.parse_one(
        target.definition, dialect="postgres"
    ).sql(dialect="postgres", pretty=True)
    # The SQL is pre-indented by 4 spaces.  alembic adds a further 4 spaces
    # when writing the migration function body, so both the content and the
    # closing """ land at 8 spaces in the final file.
    indented_sql = textwrap.indent(formatted_sql, prefix="    ")
    definition_value = f'"""\n{indented_sql}\n    """'

    return (
        f"{var_name} = {class_name}(\n"
        f'    schema="{target.schema}",\n'
        f'    signature="{target.signature}",\n'
        f"    definition={definition_value}\n"
        f")\n"
    )


def _render_create_entity(
    autogen_context: "AutogenContext", op: CreateOp
) -> str:
    autogen_context.imports.add(op.target.render_import_statement())
    return _render_entity_code(op.target) + (
        f"op.create_entity({op.target.to_variable_name()})\n"
    )


def _render_drop_entity(autogen_context: "AutogenContext", op: DropOp) -> str:
    autogen_context.imports.add(op.target.render_import_statement())
    return _render_entity_code(op.target) + (
        f"op.drop_entity({op.target.to_variable_name()})\n"
    )


def _render_replace_entity(
    autogen_context: "AutogenContext", op: ReplaceOp
) -> str:
    autogen_context.imports.add(op.target.render_import_statement())
    return _render_entity_code(op.target) + (
        f"op.replace_entity({op.target.to_variable_name()})\n"
    )


def _render_revert_entity(
    autogen_context: "AutogenContext", op: RevertOp
) -> str:
    autogen_context.imports.add(op.target.render_import_statement())
    db_target = op.target._version_to_replace  # noqa: SLF001
    if db_target is None:
        raise UnreachableException
    assert isinstance(db_target, ReplaceableEntity)  # noqa: S101
    return _render_entity_code(db_target) + (
        f"op.replace_entity({db_target.to_variable_name()})"
    )


def apply() -> None:
    """Register cave's renderers, overriding the alembic_utils defaults.

    alembic's renderer dispatcher is a simple dict — last registration wins.
    Because this is called from ``cave_alembic_hook()`` at env.py startup,
    after alembic_utils has already registered its own renderers on import,
    cave's registrations take precedence with no monkey-patching required.
    """
    renderers.dispatch_for(CreateOp, replace=True)(_render_create_entity)
    renderers.dispatch_for(DropOp, replace=True)(_render_drop_entity)
    renderers.dispatch_for(ReplaceOp, replace=True)(_render_replace_entity)
    renderers.dispatch_for(RevertOp, replace=True)(_render_revert_entity)
