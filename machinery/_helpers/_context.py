from __future__ import annotations

import asyncio
import contextvars
import dataclasses
import logging
import types
import weakref
from collections.abc import Callable, Coroutine, Generator, Hashable, MutableMapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Self, cast

from . import _protocols

type FutNames = weakref.WeakKeyDictionary[asyncio.Future[object], str]

_context_fut_names: contextvars.ContextVar[FutNames] = contextvars.ContextVar("fut_names")


def get_fut_names() -> FutNames:
    """
    Retrieve the dictionary of names for futures in this asyncio context.

    This is held in a contextvars.ContextVar and is local to specific asyncio
    loops.
    """
    try:
        return _context_fut_names.get()
    except LookupError:
        _context_fut_names.set(weakref.WeakKeyDictionary())
        return _context_fut_names.get()


@dataclasses.dataclass(frozen=True, kw_only=True)
class Tramp:
    """
    This represents customizable logic for the ``CTX`` and makes it easy to provide
    additional functionality to a ``CTX`` without making the ``CTX`` itself
    generic.

    It is recommended in your own projects to have an alias of ``CTX`` that you
    refer to so it's easy to change your project to use a different default
    implementation of Tramp later down the road.
    """

    log: logging.Logger

    def __hash__(self) -> int:
        return id(self)

    def set_future_name(self, fut: asyncio.Future[Any], *, name: str) -> None:
        get_fut_names()[fut] = name

    def get_future_name(self, fut: asyncio.Future[Any]) -> str | None:
        return get_fut_names().get(fut)

    def log_info(self, msg: str) -> None:
        self.log.info(msg)

    def log_exception(
        self,
        msg: object,
        *,
        exc_info: tuple[type[BaseException], BaseException, types.TracebackType] | None = None,
    ) -> None:
        self.log.exception(msg, exc_info=exc_info)

    def fut_to_string(
        self, f: asyncio.Future[Any] | _protocols.WithRepr, with_name: bool = True
    ) -> str:
        if not isinstance(f, asyncio.Future):
            return repr(f)

        s = ""
        if with_name:
            s = f"<Future#{self.get_future_name(f)}"
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

    def silent_reporter(self, res: _protocols.FutureStatus[Any]) -> None:
        """
        A generic reporter for asyncio tasks that doesn't log errors.

        This means that exceptions are **not** logged to the terminal and you won't
        get warnings about tasks not being looked at when they finish.
        """
        if res.cancelled():
            return

        exc = res.exception()
        if exc is None:
            res.result()

    def reporter(self, res: _protocols.FutureStatus[Any]) -> None:
        """
        A generic reporter for asyncio tasks.

        This means that exceptions are logged to the terminal and you won't
        get warnings about tasks not being looked at when they finish.

        Note that it will not report asyncio.CancelledError() or KeyboardInterrupt.
        """
        if res.cancelled():
            return

        exc = res.exception()
        if exc is None:
            res.result()
            return

        if not isinstance(exc, KeyboardInterrupt):
            if exc.__traceback__ is not None:
                self.log_exception(exc, exc_info=(type(exc), exc, exc.__traceback__))
            else:
                self.log_exception(exc)


@dataclasses.dataclass(frozen=True, kw_only=True)
class _CTXCallback[T_Ret, T_Tramp: _protocols.Tramp]:
    """
    This is a small wrapper around a callback such that we can provide a callback
    to the ``CTX`` that can only be called once despite being provided to multiple
    futures.
    """

    ctx: _protocols.CTX[T_Tramp]

    cb: _protocols.FutureCTXCallback[T_Ret, T_Tramp]
    event: asyncio.Event = dataclasses.field(init=False, default_factory=asyncio.Event)

    def __call__(self, res: _protocols.FutureStatus[T_Ret], /) -> None:
        if self.event.is_set():
            return

        self.event.set()
        self.cb(self.ctx, res)


@dataclasses.dataclass(frozen=True, kw_only=True)
class CTX[T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    An object loosely based off contexts in Go to provide a chain of dependency
    between async objects such that cancelling the parent propagates that to
    all children contexts.

    It is recommended to create an alias to this in your project so later down
    the track it is easy to define it with a different default implementation of
    the Tramp if that becomes desirable.

    The tramp is where you can add additional functionality so that the context
    itself doesn't need to be made generic.
    """

    name: str
    loop: asyncio.AbstractEventLoop
    tramp: T_Tramp

    _futs: Sequence[asyncio.Future[None]]

    _callbacks: MutableMapping[Hashable, _CTXCallback[None, T_Tramp]] = dataclasses.field(
        init=False, default_factory=dict
    )

    @classmethod
    def beginning(
        cls, *, name: str, tramp: T_Tramp, loop: asyncio.AbstractEventLoop | None = None
    ) -> Self:
        """
        Create a root level context using either the loop provided, or the current
        running loop.
        """
        if loop is None:
            loop = asyncio.get_event_loop_policy().get_event_loop()

        final_future: asyncio.Future[None] = loop.create_future()
        tramp.set_future_name(final_future, name=f"FUT{{{name}}}")
        final_future.add_done_callback(tramp.silent_reporter)

        return cls(name=name, tramp=tramp, loop=loop, _futs=(final_future,))

    def __hash__(self) -> int:
        """
        A context is unique by a combination of it's name, loop, tramp and the
        set of futures it holds onto.
        """
        return hash((self.name, self.loop, self.tramp, tuple(self._futs)))

    def __enter__(self) -> Self:
        """
        Using a context as a context manager ensures it is cancelled when out of
        scope.
        """
        return self

    def __repr__(self) -> str:
        """
        Make a slightly useful repr for the context showing the state of the
        futures it holds onto.
        """
        fut_results: list[str] = []
        for fut in self._futs:
            fut_name = self.tramp.get_future_name(fut)
            if not fut.done():
                fut_results.append(f"PENDING({fut_name})")
            elif fut.cancelled():
                fut_results.append(f"CANCELLED({fut_name})")
            elif (exc := fut.exception()) is not None:
                fut_results.append(f"EXCEPTION[{type(exc).__name__}]({fut_name})")
            else:
                fut_results.append(f"DONE({fut_name})")

        return f"CTX[{self.name}]({'|'.join(fut_results)})"

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        value: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        """
        Ensure the context is cancelled when it exits it's context manager.
        """
        self.cancel()

    def done(self) -> bool:
        """
        A context is done if any of the futures it holds onto are done.

        When child contexts are created, it is provided a reference to the futures
        held by it's parent. And futures are never added to a context after
        the context is created.

        So we can rely on this list to tell us if a parent has also completed.
        """
        return any(fut.done() for fut in self._futs)

    def set_exception(self, exc: BaseException) -> None:
        """
        Set an exception on the latest future, which represents the future of
        this context. This means we can cancel this context without affecting
        any parent context.
        """
        self._futs[-1].set_exception(exc)

    def cancel(self) -> bool:
        """
        Cancel the latest future, which represents the future of
        this context. This means we can cancel this context without affecting
        any parent context.
        """
        return self._futs[-1].cancel()

    def cancelled(self) -> bool:
        """
        Return True if this context or any parent context has been cancelled.
        """
        for fut in reversed(self._futs):
            if fut.done():
                return fut.cancelled()

        # Return as if we were calling cancelled on the root future if none of
        # the futures are complete. This allows us to replicate the behaviour
        # of calling cancelled on a real asyncio.Future.
        return self._futs[0].cancelled()

    def exception(self) -> BaseException | None:
        """
        Return the exception from the oldest parent that is done.

        If an younger parent is complete but had no exception, then no exception
        is returned, even if an older parent is complete with an exception.
        """
        for fut in reversed(self._futs):
            if fut.done():
                return fut.exception()

        # Return as if we were calling exception on the root future if none of
        # the futures are complete. This allows us to replicate the behaviour
        # of calling exception on a real asyncio.Future.
        return self._futs[0].exception()

    def add_on_done(
        self,
        cb: _protocols.FutureCTXCallback[None, T_Tramp],
        index: _protocols.FutureCallback[None] | None = None,
    ) -> _protocols.FutureCallback[None]:
        """
        This is similar to ``add_done_callback`` but we provide a callback that
        also takes in the context itself.

        We return the callback that was registered. If ``index`` is provided, then
        providing that object to ``remove_done_callback`` can also be used to
        unregister this callback from the context.

        The callback will only be called once when this context is complete.
        """
        callback = _CTXCallback[None, T_Tramp](ctx=self, cb=cb)

        if index is None:
            # This gets used by ``add_done_callback`` and so it's useful to
            # be able to unregister this callback using the callback that was
            # used in that function rather than this special wrapped version
            index = callback

        for fut in reversed(self._futs):
            if fut.done():
                # No need to register the callback if it's gonna be called
                # straight away
                fut.add_done_callback(callback)
                return callback

        self._callbacks[index] = callback

        for fut in self._futs:
            # Our _CTXCallback object means that we can register the callback
            # to all of the futures held by this context and be assured
            # the callback only gets called once even though when a parent
            # context is complete, all child futures will then be marked as
            # complete and call the callback.
            fut.add_done_callback(callback)

        return callback

    def add_done_callback(
        self, cb: Callable[[_protocols.FutureStatus[None]], None]
    ) -> _protocols.FutureCallback[None]:
        """
        Add a callback to be called when this context or any of it's parents are
        complete.

        It is provided the future that represents the state of the complete context.
        """

        def wrapped(_: _protocols.CTX[T_Tramp], res: _protocols.FutureStatus[None]) -> None:
            return cb(res)

        # Use add_on_done to ensure the callback is only ever called once.
        return self.add_on_done(wrapped, index=cb)

    def remove_done_callback(self, cb: Callable[[_protocols.FutureStatus[None]], None]) -> int:
        """
        Unregister the provided callback if it has been registered to run when
        this context is complete.

        We return the number of futures it was returned from. This number can
        be more than one depending on how many parent futures this context is
        aware of.
        """
        counts: list[int] = []
        ctx_callable = self._callbacks.pop(cb, None)
        for fut in self._futs:
            counts.append(fut.remove_done_callback(cb))
            if ctx_callable is not None:
                counts.append(fut.remove_done_callback(ctx_callable))

        if not any(counts):
            return 0

        return max(counts)

    def has_direct_done_callback(
        self, cb: Callable[[_protocols.FutureStatus[None]], None]
    ) -> bool:
        """
        Return whether this context has this callback registered.

        If the callback is registered in a parent context, then this will not
        return True.
        """
        return cb in self._callbacks

    async def wait_for_first(self, *waits: _protocols.WaitByCallback[Any] | asyncio.Event) -> None:
        """
        Given a number of futures, tasks or events, return when at least one of them
        is complete.

        If any are provided and one is already is complete then we will at least
        do an ``await asyncio.sleep(0)`` before returning.
        """
        if not waits:
            return

        waiter = asyncio.Event()

        any_events_done = any(isinstance(wait, asyncio.Event) and wait.is_set() for wait in waits)
        any_futures_done = any(
            not isinstance(wait, asyncio.Event) and wait.done() for wait in waits
        )

        if any_events_done or any_futures_done:
            # Ensure that at least one cycle of the event loop is run even though
            # at least one of our waits is complete
            await asyncio.sleep(0)
            return

        futs: list[_protocols.WaitByCallback[Any]] = []
        tasks: list[asyncio.Task[Literal[True]]] = []
        for wait in waits:
            if isinstance(wait, asyncio.Event):
                # We need to create a task to track events
                # We then ensure at the end that all these tasks are cancelled
                # And awaited before exiting
                task = self.loop.create_task(wait.wait())
                tasks.append(task)
                futs.append(task)
            else:
                futs.append(wait)

        unique = list({id(fut): fut for fut in futs}.values())

        def done(res: object) -> None:
            waiter.set()

        for fut in unique:
            fut.add_done_callback(done)

        try:
            await waiter.wait()
        finally:
            for fut in unique:
                fut.remove_done_callback(done)
            for task in tasks:
                task.cancel()
            await self.wait_for_all(*tasks)

    async def wait_for_all(self, *waits: _protocols.WaitByCallback[Any] | asyncio.Event) -> None:
        """
        Wait for all the futures to be complete and return without error regardless
        of whether the futures completed successfully or not.

        If there are no futures, nothing is done and we return without error.
        """
        if not waits:
            return

        all_events_done = all(isinstance(wait, asyncio.Event) and wait.is_set() for wait in waits)
        all_futures_done = all(
            not isinstance(wait, asyncio.Event) and wait.done() for wait in waits
        )

        waiter = asyncio.Event()

        if all_events_done and all_futures_done:
            # Ensure that at least one cycle of the event loop is run even though
            # all our waits are already completed
            await asyncio.sleep(0)
            return

        futs: list[_protocols.WaitByCallback[Any]] = []
        tasks: list[asyncio.Task[Literal[True]]] = []
        for wait in waits:
            if isinstance(wait, asyncio.Event):
                # We need to create a task to track events
                # We then ensure at the end that all these tasks are cancelled
                # And awaited before exiting
                task = self.loop.create_task(wait.wait())
                tasks.append(task)
                futs.append(task)
            else:
                futs.append(wait)

        unique = list({id(fut): fut for fut in futs}.values())
        complete: dict[int, bool] = {}

        def done(res: object) -> None:
            complete[id(res)] = True
            if len(complete) == len(unique):
                waiter.set()

        for fut in unique:
            fut.add_done_callback(done)

        try:
            await waiter.wait()
        finally:
            for fut in unique:
                fut.remove_done_callback(done)
            for task in tasks:
                task.cancel()
            await self.wait_for_all(*tasks)

    async def async_with_timeout[T_Ret](
        self,
        coro: Coroutine[object, object, T_Ret],
        *,
        name: str,
        silent: bool = True,
        timeout: int = 10,
        timeout_event: asyncio.Event | None = None,
        timeout_error: BaseException | None = None,
    ) -> T_Ret:
        """
        Run a coroutine as a task until it's complete or times out.

        If time runs out the task is cancelled.

        If timeout_error is defined, that is raised instead of asyncio.CancelledError
        on timeout.

        If a ``timeout_event`` is provided, then it is set when the timeout occurs
        if the task is  still running. It is never set if the timeout is reached
        after the task is complete.

        This function does not return until the task has finished cleanup.
        """
        result: asyncio.Future[T_Ret] = self.loop.create_future()
        result.add_done_callback(self.tramp.silent_reporter)
        self.tramp.set_future_name(result, name=f"RESULT_WITH_TIMEOUT{{{self.name}}}({name})")

        task = self.async_as_background(coro, silent=silent)

        def pass_result(res: _protocols.FutureStatus[T_Ret]) -> None:
            if result.done():
                return

            if res.cancelled():
                result.cancel()
                return

            if (exc := res.exception()) is not None:
                result.set_exception(exc)
                return

            result.set_result(res.result())

        task.add_done_callback(pass_result)

        def set_timeout() -> None:
            if task.done():
                return

            if timeout_event is not None:
                if not timeout_event.is_set():
                    timeout_event.set()

            if result.done():
                return

            if timeout_error:
                result.set_exception(timeout_error)
            else:
                result.cancel()

            task.cancel()

        handle = self.loop.call_later(timeout, set_timeout)
        try:
            return await result
        finally:
            handle.cancel()
            if not task.done():
                task.cancel()
            await self.wait_for_all(task)

    def async_as_background[T_Ret](
        self, coro: Coroutine[object, object, T_Ret], *, silent: bool = True
    ) -> asyncio.Task[T_Ret]:
        """
        Create an asyncio.Task from the provided coroutine and provide either
        ``tramp.reporter`` or ``tramp.silent_reporter`` as a done callback
        depending on the value of ``silent``.

        It is up to the caller to ensure that the task is awaited before the
        program is finished to avoid asyncio complaining about a task that is
        never awaited.
        """
        task = self.loop.create_task(coro)

        if silent:
            task.add_done_callback(self.tramp.silent_reporter)
        else:
            task.add_done_callback(self.tramp.reporter)

        return task

    def child(self, *, name: str, prefix: str = "") -> Self:
        """
        Create a child context with the provided name and prefix.

        If prefix is provided then the name will be ``[{prefix}]-->{name}``

        The child context will be provided the tramp that is on this context
        and will know about all the futures held by this context.
        """
        if prefix:
            prefix = f"[{prefix}]-->"

        final_future: asyncio.Future[None] = self.loop.create_future()
        self.tramp.set_future_name(final_future, name=f"{prefix}FUT{{{self.name}-->{name}}}")
        final_future.add_done_callback(self.tramp.silent_reporter)

        return dataclasses.replace(
            self,
            name=f"{self.name}-->{prefix}{name}",
            tramp=self.tramp,
            _futs=tuple([*self._futs, final_future]),
        )

    def __await__(self) -> Generator[None]:
        """
        Wait for this context to be complete
        """
        return self._wait().__await__()

    async def _wait(self) -> None:
        """
        The logic used by ``__await__`` to not return until all the futures
        recognised by this context are complete
        """
        for fut in reversed(self._futs):
            if fut.done():
                await fut
                return

        await self.wait_for_all(self)

        for fut in reversed(self._futs):
            if fut.done():
                await fut
                return


if TYPE_CHECKING:
    _T: _protocols.Tramp = cast(Tramp, None)
    _CTX: _protocols.CTX[Tramp] = cast(CTX[Tramp], None)
    _WBC: _protocols.WaitByCallback[None] = cast(CTX[Tramp], None)
    _CB: _protocols.FutureCallback[None] = cast(_CTXCallback[None, Tramp], None)
