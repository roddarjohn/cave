"""Utility decorators for dimension factory functions."""

from collections.abc import Callable
from functools import wraps


class CaveValidationError(Exception):
    """Raised when a cave validation check fails."""


def raise_exception_on_false[**P](
    func: Callable[P, bool],
) -> Callable[P, bool]:
    """Wrap a validator function to raise CaveValidationError on failure."""

    @wraps(func)
    def inner(*args: P.args, **kwargs: P.kwargs) -> bool:
        valid = func(*args, **kwargs)

        if not valid:
            msg = f"{getattr(func, '__name__', repr(func))} failed"
            raise CaveValidationError(msg)

        return valid

    return inner
