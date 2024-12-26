import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Protocol, TypeVar

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


class ResettableFuture:
    """
    A future object with a ``reset()`` function that resets it

    Usage:

    .. code-block:: python

        fut = ResettableFuture()
        fut.set_result(True)
        await fut == True

        fut.reset()
        fut.set_result(False)
        await fut == False

    Calling reset on one of these will do nothing if it already isn't resolved.

    Calling reset on a resolved future will also remove any registered done
    callbacks.
    """

    _asyncio_future_blocking = False

    def __init__(self, name=None):
        self.name = name
        self.fut = create_future(name=f"ResettableFuture({self.name})::__init__[fut]")

    def reset(self, force=False):
        if force:
            self.fut.cancel()

        if not self.fut.done():
            return

        self.fut = create_future(name=f"ResettableFuture({self.name})::reset[fut]")

    @property
    def _callbacks(self):
        return self.fut._callbacks

    def set_result(self, data):
        self.fut.set_result(data)

    def set_exception(self, exc):
        self.fut.set_exception(exc)

    def cancel(self):
        self.fut.cancel()

    def result(self):
        return self.fut.result()

    def done(self):
        return self.fut.done()

    def cancelled(self):
        return self.fut.cancelled()

    def exception(self):
        return self.fut.exception()

    def add_done_callback(self, func):
        self.fut.add_done_callback(func)

    def remove_done_callback(self, func):
        self.fut.remove_done_callback(func)

    def __repr__(self):
        return f"<ResettableFuture#{self.name}({fut_to_string(self.fut, with_name=False)})>"

    def __await__(self):
        return (yield from self.fut)

    __iter__ = __await__


class ChildOfFuture:
    """
    Create a future that also considers the status of it's parent.

    So if the parent is cancelled, then this future is cancelled.
    If the parent raises an exception, then that exception is given to this result

    The special case is if the parent receives a result, then this future is
    cancelled.

    The recommended use is with it's context manager::

        from machinery import helpers as hp

        parent_fut = hp.create_future()

        with hp.ChildOfFuture(parent_fut):
            ...

    If you don't use the context manager then ensure you resolve the future when
    you no longer need it (i.e. ``fut.cancel()``) to avoid a memory leak.
    """

    _asyncio_future_blocking = False

    def __init__(self, original_fut, *, name=None):
        self.name = name
        self.fut = create_future(name=f"ChildOfFuture({self.name})::__init__[fut]")
        self.original_fut = original_fut

        self.fut.add_done_callback(self.remove_parent_done)
        self.original_fut.add_done_callback(self.parent_done)

    def __enter__(self):
        return self

    def __exit__(self, exc_typ, exc, tb):
        self.cancel()

    def parent_done(self, res):
        if self.fut.done():
            return

        if res.cancelled():
            self.fut.cancel()
            return

        exc = res.exception()
        if exc:
            self.fut.set_exception(exc)
        else:
            self.fut.cancel()

    def remove_parent_done(self, ret):
        self.original_fut.remove_done_callback(self.parent_done)

    @property
    def _callbacks(self):
        return self.fut._callbacks

    def set_result(self, data):
        if self.original_fut.done():
            self.original_fut.set_result(data)
        self.fut.set_result(data)

    def set_exception(self, exc):
        if self.original_fut.done():
            self.original_fut.set_exception(exc)
        self.fut.set_exception(exc)

    def cancel(self):
        self.fut.cancel()

    def result(self):
        if self.original_fut.done() or self.original_fut.cancelled():
            if self.original_fut.cancelled():
                return self.original_fut.result()
            else:
                self.fut.cancel()
        if self.fut.done() or self.fut.cancelled():
            return self.fut.result()
        return self.original_fut.result()

    def done(self):
        return self.fut.done() or self.original_fut.done()

    def cancelled(self):
        if self.fut.cancelled() or self.original_fut.cancelled():
            return True

        # We cancel fut if original_fut gets a result
        if self.original_fut.done() and not self.original_fut.exception():
            self.fut.cancel()
            return True

        return False

    def exception(self):
        if self.fut.done() and not self.fut.cancelled():
            exc = self.fut.exception()
            if exc is not None:
                return exc

        if self.original_fut.done() and not self.original_fut.cancelled():
            exc = self.original_fut.exception()
            if exc is not None:
                return exc

        if self.fut.cancelled() or self.fut.done():
            return self.fut.exception()

        return self.original_fut.exception()

    def cancel_parent(self):
        if hasattr(self.original_fut, "cancel_parent"):
            self.original_fut.cancel_parent()
        else:
            self.original_fut.cancel()

    def add_done_callback(self, func):
        self.fut.add_done_callback(func)

    def remove_done_callback(self, func):
        self.fut.remove_done_callback(func)

    def __repr__(self):
        return f"<ChildOfFuture#{self.name}({fut_to_string(self.fut, with_name=False)}){fut_to_string(self.original_fut)}>"

    def __await__(self):
        return (yield from self.fut)

    __iter__ = __await__


def fut_has_callback(fut, callback):
    """
    Look at the callbacks on the future and return ``True`` if any of them
    are the provided ``callback``.
    """
    if not fut._callbacks:
        return False

    for cb in fut._callbacks:
        if type(cb) is tuple and cb:
            cb = cb[0]

        if cb == callback:
            return True

    return False


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


async def async_with_timeout(
    coroutine, *, timeout=10, timeout_error=None, silent=False, name=None
):
    """
    Run a coroutine as a task until it's complete or times out.

    If time runs out the task is cancelled.

    If timeout_error is defined, that is raised instead of asyncio.CancelledError
    on timeout.

    .. code-block:: python

        from machinery.helpers import hp

        import asyncio


        async def my_coroutine():
            await asyncio.sleep(120)

        await hp.async_with_timeout(my_coroutine(), timeout=20)
    """
    f = create_future(name=f"||async_with_timeout({name})[final]")
    t = async_as_background(coroutine, silent=silent)

    def pass_result(res):
        if res.cancelled():
            f.cancel()
            return

        if res.exception() is not None:
            if not f.done():
                f.set_exception(t.exception())
            return

        if t.done() and not f.done():
            f.set_result(t.result())

    t.add_done_callback(pass_result)

    def set_timeout():
        if not t.done():
            if timeout_error and not f.done():
                f.set_exception(timeout_error)

            t.cancel()
            f.cancel()

    handle = get_event_loop().call_later(timeout, set_timeout)
    try:
        return await f
    finally:
        handle.cancel()


async def wait_for_all_futures(*futs, name=None):
    """
    Wait for all the futures to be complete and return without error regardless
    of whether the futures completed successfully or not.

    If there are no futures, nothing is done and we return without error.

    We determine all the futures are done when the number of completed futures
    is equal to the number of futures we started with. This is to ensure if a
    future is special and calling done() after the future callback has been
    called is not relevant anymore, we still count the future as done.
    """
    if not futs:
        return

    waiter = create_future(name=f"||wait_for_all_futures({name})[waiter]")

    unique = {id(fut): fut for fut in futs}.values()
    complete = {}

    def done(res):
        complete[id(res)] = True
        if not waiter.done() and len(complete) == len(unique):
            waiter.set_result(True)

    for fut in unique:
        fut.add_done_callback(done)

    try:
        await waiter
    finally:
        for fut in unique:
            fut.remove_done_callback(done)


async def wait_for_first_future(*futs, name=None):
    """
    Return without error when the first future to be completed is done.
    """
    if not futs:
        return

    waiter = create_future(name=f"||wait_for_first_future({name})[waiter]")
    unique = {id(fut): fut for fut in futs}.values()

    def done(res):
        if not waiter.done():
            waiter.set_result(True)

    for fut in unique:
        fut.add_done_callback(done)

    try:
        await waiter
    finally:
        for fut in unique:
            fut.remove_done_callback(done)


async def cancel_futures_and_wait(*futs, name=None):
    """
    Cancel the provided futures and wait for them all to finish. We will still
    await the futures if they are all already done to ensure no warnings about
    futures being destroyed while still pending.
    """
    if not futs:
        return

    waiting = []

    for fut in futs:
        if not fut.done():
            fut.cancel()
            waiting.append(fut)

    await wait_for_all_futures(
        *waiting, name=f"||cancel_futures_and_wait({name})[wait_for_everything]"
    )


def transfer_result(fut, errors_only=False, process=None):
    """
    Return a ``done_callback`` that transfers the result, errors or cancellation
    to the provided future.

    If errors_only is ``True`` then it will not transfer a successful result
    to the provided future.

    .. code-block:: python

        from machinery import helpers as hp

        import asyncio


        async def my_coroutine():
            return 2

        fut = hp.create_future()
        task = hp.async_as_background(my_coroutine())
        task.add_done_callback(hp.transfer_result(fut))

        assert (await fut) == 2

    If process is provided, then when the coroutine is done, process will be
    called with the result of the coroutine and the future that result is being
    transferred to.
    """

    def transfer(res):
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


def noncancelled_results_from_futs(futs):
    """
    Get back (exception, results) from a list of futures

    exception
        A single exception if all the errors are the same type
        or if there is only one exception

        otherwise it is None

    results
        A list of the results that exist
    """
    errors = []
    results = []
    for f in futs:
        if f.done() and not f.cancelled():
            exc = f.exception()
            if exc:
                if not any(e == exc for e in errors):
                    errors.append(exc)
            else:
                results.append(f.result())

    if errors:
        errors = list(errors)
        if len(errors) == 1:
            errors = errors[0]
        else:
            errors = ExceptionGroup("Futures failed", errors)
    else:
        errors = None

    return (errors, results)


def find_and_apply_result(final_fut, available_futs):
    """
    Find a result in available_futs with a result or exception and set that
    result/exception on both final_fut, and all the settable futs
    in available_futs.

    As a bonus, if final_fut is set, then we set that result/exception on
    available_futs.

    and if final_fut is cancelled, we cancel all the available futs

    Return True if we've changed final_fut
    """
    if final_fut.cancelled():
        for f in available_futs:
            f.cancel()
        return False

    if final_fut.done():
        current_exc = final_fut.exception()
        if current_exc:
            for f in available_futs:
                if not f.done():
                    f.set_exception(current_exc)
            return False

    errors, results = noncancelled_results_from_futs(available_futs)
    if errors:
        for f in available_futs:
            if not f.done() and not f.cancelled():
                f.set_exception(errors)
        if not final_fut.done():
            final_fut.set_exception(errors)
            return True
        return False

    if results:
        res = results[0]
        for f in available_futs:
            if not f.done() and not f.cancelled():
                f.set_result(res)
        if not final_fut.done():
            final_fut.set_result(res)
            return True
        return False

    for f in available_futs:
        if f.cancelled():
            final_fut.cancel()
            return True

    return False
