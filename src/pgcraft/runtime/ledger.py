"""Runtime helpers for ledger state recording.

Provides context managers that wrap the begin/apply lifecycle of a
:class:`~pgcraft.ledger.actions.StateAction`.

Usage::

    reconcile = StateAction(
        name="reconcile", diff_keys=["sku", "warehouse"]
    )
    LedgerResourceFactory(
        "inventory", "ops", metadata, [...], actions=[reconcile]
    )

    # Later, at runtime:
    with LedgerStateRecorder(
        session, reconcile, reason="monthly_sync"
    ) as staging:
        session.execute(
            staging.insert(),
            [{"sku": "A", "warehouse": "NYC", "value": 100}],
        )
    # apply is called automatically on clean exit

For async code use :class:`AsyncLedgerStateRecorder` with
``async with``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy import Table
    from sqlalchemy.orm import Session

    from pgcraft.ledger.actions import StateAction


class LedgerStateRecorder:
    """Synchronous context manager for StateAction begin/apply lifecycle.

    On entry (``__enter__``), calls the ``_begin`` function to create
    and truncate the staging temp table, then returns the staging
    :class:`~sqlalchemy.Table` for the caller to insert desired-state
    rows into.

    On clean exit (``__exit__`` with no exception), calls the
    ``_apply`` function to compute and insert correcting delta rows
    and truncates the staging table.

    On exceptional exit the staging table is left as-is; the
    surrounding transaction rollback handles cleanup.

    Args:
        session: An open SQLAlchemy :class:`~sqlalchemy.orm.Session`.
        action: A :class:`~pgcraft.ledger.actions.StateAction` whose
            ``_begin_fn``, ``_apply_fn``, and ``_staging_table``
            have been populated by
            :class:`~pgcraft.plugins.ledger_actions.LedgerActionsPlugin`.
        **apply_kwargs: Write-only key values forwarded as named
            parameters to the apply function.

    Raises:
        RuntimeError: If *action* has not been wired up by the plugin
            (i.e. ``_begin_fn`` is ``None``).

    """

    def __init__(
        self,
        session: Session,
        action: StateAction,
        **apply_kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Store configuration."""
        if (
            action._begin_fn is None  # noqa: SLF001
            or action._apply_fn is None  # noqa: SLF001
        ):
            msg = (
                f"StateAction {action.name!r} has not been processed "
                f"by LedgerActionsPlugin.  Pass it to "
                f"LedgerResourceFactory via the actions= parameter "
                f"before using it at runtime."
            )
            raise RuntimeError(msg)
        self._session = session
        self._action = action
        self._apply_kwargs = apply_kwargs

    def __enter__(self) -> Table:
        """Call the begin function and return the staging table.

        Returns:
            The staging :class:`~sqlalchemy.Table` (isolated MetaData).
            Use ``session.execute(staging.insert(), rows)`` to
            populate it.

        """
        begin_fn = self._action._begin_fn  # noqa: SLF001
        self._session.execute(text(f"SELECT {begin_fn}()"))
        return self._action._staging_table  # type: ignore[return-value]  # noqa: SLF001

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Call the apply function on clean exit.

        If an exception is in flight the apply is skipped; the
        surrounding transaction rollback handles cleanup.

        """
        if exc_type is not None:
            return

        apply_fn = self._action._apply_fn  # noqa: SLF001
        write_only_keys = self._action.write_only_keys

        if write_only_keys:
            placeholders = ", ".join(f":{k}" for k in write_only_keys)
            sql = text(f"SELECT * FROM {apply_fn}({placeholders})")  # noqa: S608
            params = {k: self._apply_kwargs.get(k) for k in write_only_keys}
            self._session.execute(sql, params)
        else:
            self._session.execute(
                text(f"SELECT * FROM {apply_fn}()")  # noqa: S608
            )


class AsyncLedgerStateRecorder:
    """Async context manager for StateAction begin/apply lifecycle.

    Identical to :class:`LedgerStateRecorder` but uses
    ``await session.execute(...)`` for async SQLAlchemy sessions.

    Args:
        session: An open SQLAlchemy async session
            (``AsyncSession``).
        action: A :class:`~pgcraft.ledger.actions.StateAction`
            wired up by the plugin.
        **apply_kwargs: Write-only key values forwarded to apply.

    Raises:
        RuntimeError: If *action* has not been wired up by the plugin.

    """

    def __init__(
        self,
        session: Any,  # noqa: ANN401
        action: StateAction,
        **apply_kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Store configuration."""
        if (
            action._begin_fn is None  # noqa: SLF001
            or action._apply_fn is None  # noqa: SLF001
        ):
            msg = (
                f"StateAction {action.name!r} has not been processed "
                f"by LedgerActionsPlugin.  Pass it to "
                f"LedgerResourceFactory via the actions= parameter "
                f"before using it at runtime."
            )
            raise RuntimeError(msg)
        self._session = session
        self._action = action
        self._apply_kwargs = apply_kwargs

    async def __aenter__(self) -> Table:
        """Call the begin function and return the staging table.

        Returns:
            The staging :class:`~sqlalchemy.Table`.

        """
        begin_fn = self._action._begin_fn  # noqa: SLF001
        await self._session.execute(text(f"SELECT {begin_fn}()"))
        return self._action._staging_table  # type: ignore[return-value]  # noqa: SLF001

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Call the apply function on clean exit."""
        if exc_type is not None:
            return

        apply_fn = self._action._apply_fn  # noqa: SLF001
        write_only_keys = self._action.write_only_keys

        if write_only_keys:
            placeholders = ", ".join(f":{k}" for k in write_only_keys)
            sql = text(f"SELECT * FROM {apply_fn}({placeholders})")  # noqa: S608
            params = {k: self._apply_kwargs.get(k) for k in write_only_keys}
            await self._session.execute(sql, params)
        else:
            await self._session.execute(
                text(f"SELECT * FROM {apply_fn}()")  # noqa: S608
            )
