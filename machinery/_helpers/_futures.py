import asyncio
from collections.abc import AsyncGenerator, Sequence

from . import _protocols


async def stop_async_generator[T_Send](
    gen: AsyncGenerator[object, T_Send | None],
    provide: T_Send | None = None,
    exc: BaseException | None = None,
) -> None:
    """
    This will ensure an async generator has stopped.

    It will first throw an exception into the generator using ``gen.athrow``.
    This is either an ``asyncio.CancelledError`` or the ``exc`` that is provided
    if that is not ``None``.

    Then we will send either ``provide`` with ``gen.asend``.

    Finally, regardless of exceptions, ``gen.aclose`` will be called.
    """
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
