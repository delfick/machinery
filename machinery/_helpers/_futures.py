import asyncio
import logging
from collections.abc import AsyncGenerator, Callable, Sequence
from typing import Protocol, TypeVar

from . import _protocols

T_Send = TypeVar("T_Send")

log = logging.getLogger("machinery.helpers")


class _WithRepr(Protocol):
    def __repr__(self) -> str: ...


async def stop_async_generator(
    gen: AsyncGenerator[object, T_Send | None],
    provide: T_Send | None = None,
    name: str | None = None,
    exc: BaseException | None = None,
) -> None:
    try:
        try:
            await gen.athrow(exc or asyncio.CancelledError())
        except StopAsyncIteration:
            pass

        try:
            await gen.asend(provide)
        except StopAsyncIteration:
            pass
    finally:
        await gen.aclose()


def fut_to_string(f: asyncio.Future[object] | _WithRepr, with_name: bool = True) -> str:
    if not isinstance(f, asyncio.Future):
        s = repr(f)
    else:
        s = ""
        if with_name:
            s = f"<Future#{getattr(f, 'name', None)}"
        if not f.done():
            s = f"{s}(pending)"
        elif f.cancelled():
            s = f"{s}(cancelled)"
        else:
            exc = f.exception()
            if exc:
                s = f"{s}(exception:{type(exc).__name__}:{exc})"
            else:
                s = f"{s}(result)"
        if with_name:
            s = f"{s}>"
    return s


def silent_reporter(res):
    """
    A generic reporter for asyncio tasks that doesn't log errors.

    For example:

    .. code-block:: python

        t = loop.create_task(coroutine())
        t.add_done_callback(hp.silent_reporter)

    This means that exceptions are **not** logged to the terminal and you won't
    get warnings about tasks not being looked at when they finish.

    This method will return ``True`` if there was no exception and ``None``
    otherwise.

    It also handles and silences ``asyncio.CancelledError``.
    """
    if not res.cancelled():
        exc = res.exception()
        if not exc:
            res.result()
            return True


def reporter(res):
    """
    A generic reporter for asyncio tasks.

    For example:

    .. code-block:: python

        t = loop.create_task(coroutine())
        t.add_done_callback(hp.reporter)

    This means that exceptions are logged to the terminal and you won't
    get warnings about tasks not being looked at when they finish.

    This method will return ``True`` if there was no exception and ``None``
    otherwise.

    It also handles and silences ``asyncio.CancelledError``.
    """
    if not res.cancelled():
        exc = res.exception()
        if exc:
            if not isinstance(exc, KeyboardInterrupt):
                log.exception(exc, exc_info=(type(exc), exc, exc.__traceback__))
        else:
            res.result()
            return True


def get_event_loop():
    return asyncio.get_event_loop_policy().get_event_loop()


def create_future(*, name=None, loop=None):
    if loop is None:
        loop = get_event_loop()
    future = loop.create_future()
    future.name = name
    future.add_done_callback(silent_reporter)
    return future


def fut_has_callback[T_Ret](
    fut: asyncio.Future[T_Ret], callback: _protocols.FutureCallback[T_Ret]
) -> bool:
    """
    Look at the callbacks on the future and return ``True`` if any of them
    are the provided ``callback``.
    """
    if not fut._callbacks:
        return False
    return any(cb == callback for cb, _ in fut._callbacks)


def async_as_background(coroutine, silent=False):
    """
    Create a task with :func:`reporter` as a done callback and return the created
    task. If ``silent=True`` then use :func:`silent_reporter`.

    This is useful because if a task exits with an exception, but nothing ever
    retrieves that exception then Python will print annoying warnings about this.

    .. code-block:: python

        from machinery import helpers as hp


        async def my_func():
            await something()

        # Kick off the function in the background
        hp.async_as_background(my_func())
    """
    t = get_event_loop().create_task(coroutine)
    if silent:
        t.add_done_callback(silent_reporter)
    else:
        t.add_done_callback(reporter)
    return t


def transfer_result[T_Res](
    fut: asyncio.Future[T_Res],
    errors_only: bool = False,
    process: Callable[[_protocols.FutureStatus[T_Res], asyncio.Future[T_Res]], None] | None = None,
) -> _protocols.FutureCallback[T_Res]:
    """
    Return a ``done_callback`` that transfers the result, errors or cancellation
    to the provided future.

    If errors_only is ``True`` then it will not transfer a successful result
    to the provided future.

    If process is provided, then when the coroutine is done, process will be
    called with the result of the coroutine and the future that result is being
    transferred to.
    """

    def transfer(res: _protocols.FutureStatus[T_Res]) -> None:
        if res.cancelled():
            fut.cancel()
            return

        exc = res.exception()

        if fut.done():
            return

        if exc is not None:
            fut.set_exception(exc)
            return

        if not errors_only:
            fut.set_result(res.result())

        if process:
            process(res, fut)

    return transfer


def noncancelled_results_from_futs[T_Ret](
    futs: Sequence[_protocols.FutureStatus[T_Ret]],
) -> tuple[BaseException | BaseExceptionGroup | None, Sequence[T_Ret]]:
    """
    Get back (exception, results) from a list of futures

    exception
        A single exception if all the errors are the same type
        or if there is only one exception

        otherwise it is None

    results
        A list of the results that exist
    """
    errors: list[BaseException] = []
    results: list[T_Ret] = []

    for f in futs:
        if f.done() and not f.cancelled():
            exc = f.exception()
            if exc:
                if not any(e == exc for e in errors):
                    errors.append(exc)
            else:
                results.append(f.result())

    final_error: BaseException | BaseExceptionGroup | None = None
    if errors:
        if len(errors) == 1:
            final_error = errors[0]
        else:
            final_error = BaseExceptionGroup("Futures failed", errors)

    return (final_error, results)


def find_and_apply_result[T_Ret](
    final: asyncio.Future[T_Ret], futs: Sequence[asyncio.Future[T_Ret]]
) -> bool:
    """
    Find a result in ``futs`` with a result or exception and set that
    result/exception on both ``final``, and all the settable futures
    in ``futs``.

    As a bonus, if ``final`` is set, then we set that result/exception on
    all futures in ``futs``.

    and if ``final`` is cancelled, we cancel all the futures in ``futs``

    Return True if we've changed ``final``
    """
    if final.cancelled():
        for f in futs:
            f.cancel()
        return False

    if final.done():
        current_exc = final.exception()
        if current_exc:
            for f in futs:
                if not f.done():
                    f.set_exception(current_exc)
            return False

    errors, results = noncancelled_results_from_futs(futs)
    if errors:
        for f in futs:
            if not f.done() and not f.cancelled():
                f.set_exception(errors)
        if not final.done():
            final.set_exception(errors)
            return True
        return False

    if results:
        res = results[0]
        for f in futs:
            if not f.done() and not f.cancelled():
                f.set_result(res)
        if not final.done():
            final.set_result(res)
            return True
        return False

    for f in futs:
        if f.cancelled():
            final.cancel()
            return True

    return False
