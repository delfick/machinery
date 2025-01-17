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
    try:
        return _context_fut_names.get()
    except LookupError:
        _context_fut_names.set(weakref.WeakKeyDictionary())
        return _context_fut_names.get()


@dataclasses.dataclass(frozen=True, kw_only=True)
class Tramp:
    log: logging.Logger

    def __hash__(self) -> int:
        return id(self)

    def set_future_name(self, fut: asyncio.Future[Any], *, name: str) -> None:
        get_fut_names()[fut] = name

    def get_future_name(self, fut: asyncio.Future[Any]) -> str | None:
        return get_fut_names().get(fut)

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

    def silent_reporter(self, res: _protocols.FutureStatus[Any]) -> Literal[True] | None:
        """
        A generic reporter for asyncio tasks that doesn't log errors.

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

        return None

    def reporter(self, res: _protocols.FutureStatus[Any]) -> Literal[True] | None:
        """
        A generic reporter for asyncio tasks.

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
                    if exc.__traceback__ is not None:
                        self.log_exception(exc, exc_info=(type(exc), exc, exc.__traceback__))
                    else:
                        self.log_exception(exc)
            else:
                res.result()
                return True

        return None


@dataclasses.dataclass(frozen=True, kw_only=True)
class _CTXCallback[T_Ret, T_Tramp: _protocols.Tramp]:
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
    name: str
    loop: asyncio.AbstractEventLoop
    tramp: T_Tramp

    futs: Sequence[asyncio.Future[None]]

    _callbacks: MutableMapping[Hashable, _CTXCallback[None, T_Tramp]] = dataclasses.field(
        init=False, default_factory=dict
    )

    @classmethod
    def beginning(
        cls, *, name: str, tramp: T_Tramp, loop: asyncio.AbstractEventLoop | None = None
    ) -> Self:
        if loop is None:
            loop = asyncio.get_event_loop_policy().get_event_loop()

        final_future: asyncio.Future[None] = loop.create_future()
        tramp.set_future_name(final_future, name=f"FUT{{{name}}}")
        final_future.add_done_callback(tramp.silent_reporter)

        return cls(name=name, tramp=tramp, loop=loop, futs=(final_future,))

    def __hash__(self) -> int:
        return hash((self.name, self.loop, self.tramp, tuple(self.futs)))

    def __enter__(self) -> Self:
        return self

    def __repr__(self) -> str:
        fut_results: list[str] = []
        for fut in self.futs:
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
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        self.cancel()

    def done(self) -> bool:
        return any(fut.done() for fut in self.futs)

    def set_exception(self, exc: BaseException) -> None:
        self.futs[-1].set_exception(exc)

    def cancel(self) -> bool:
        return self.futs[-1].cancel()

    def cancelled(self) -> bool:
        for fut in reversed(self.futs):
            if fut.done():
                return fut.cancelled()

        return self.futs[0].cancelled()

    def exception(self) -> BaseException | None:
        for fut in reversed(self.futs):
            if fut.done():
                return fut.exception()

        return self.futs[0].exception()

    def add_on_done(
        self,
        cb: _protocols.FutureCTXCallback[None, T_Tramp],
        index: _protocols.FutureCallback[None] | None = None,
    ) -> _protocols.FutureCallback[None]:
        callback = _CTXCallback[None, T_Tramp](ctx=self, cb=cb)

        if index is None:
            index = callback

        for fut in reversed(self.futs):
            if fut.done():
                fut.add_done_callback(callback)
                return callback

        self._callbacks[index] = callback

        for fut in self.futs:
            fut.add_done_callback(callback)

        return callback

    def add_done_callback(
        self, cb: Callable[[_protocols.FutureStatus[None]], None]
    ) -> _protocols.FutureCallback[None]:
        def wrapped(_: _protocols.CTX[T_Tramp], res: _protocols.FutureStatus[None]) -> None:
            return cb(res)

        return self.add_on_done(wrapped, index=cb)

    def remove_done_callback(self, cb: Callable[[_protocols.FutureStatus[None]], None]) -> int:
        counts: list[int] = []
        ctx_callable = self._callbacks.pop(cb, None)
        for fut in self.futs:
            counts.append(fut.remove_done_callback(cb))
            if ctx_callable is not None:
                counts.append(fut.remove_done_callback(ctx_callable))

        if not any(counts):
            return 0

        return max(counts)

    async def wait_for_first_future(self, *futs: _protocols.WaitByCallback[Any]) -> None:
        """
        Return without error when the first future to be completed is done.
        """
        if not futs:
            return

        unique = list({id(fut): fut for fut in futs}.values())

        if any(fut.done() for fut in unique):
            return

        waiter = asyncio.Event()

        def done(res: object) -> None:
            waiter.set()

        for fut in unique:
            fut.add_done_callback(done)

        try:
            await waiter.wait()
        finally:
            for fut in unique:
                fut.remove_done_callback(done)

    async def wait_for_all_futures(self, *futs: _protocols.WaitByCallback[Any]) -> None:
        """
        Wait for all the futures to be complete and return without error regardless
        of whether the futures completed successfully or not.

        If there are no futures, nothing is done and we return without error.
        """
        if not futs:
            return

        unique = list({id(fut): fut for fut in futs}.values())

        if all(fut.done() for fut in unique):
            return

        waiter = asyncio.Event()

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

    async def cancel_futures_and_wait(self, *futs: _protocols.WaitByCallback[Any]) -> None:
        """
        Cancel the provided futures and wait for them all to finish.
        """
        if not futs:
            return

        for fut in futs:
            fut.cancel()

        await self.wait_for_all_futures(*futs)

    async def async_with_timeout[T_Ret](
        self,
        coro: Coroutine[object, object, T_Ret],
        silent: bool = True,
        *,
        name: str,
        timeout: int = 10,
        timeout_error: BaseException | None = None,
    ) -> T_Ret:
        """
        Run a coroutine as a task until it's complete or times out.

        If time runs out the task is cancelled.

        If timeout_error is defined, that is raised instead of asyncio.CancelledError
        on timeout.
        """
        result: asyncio.Future[T_Ret] = self.loop.create_future()
        result.add_done_callback(self.tramp.silent_reporter)
        self.tramp.set_future_name(result, name=f"RESULT_WITH_TIMEOUT{{{self.name}}}({name})")

        task = self.async_as_background(coro, silent=silent)

        def pass_result(res: _protocols.FutureStatus[T_Ret]) -> None:
            if res.cancelled():
                result.cancel()
                return

            if (exc := res.exception()) is not None:
                if not result.done():
                    result.set_exception(exc)
                return

            if res.done() and not result.done():
                result.set_result(res.result())

        task.add_done_callback(pass_result)

        def set_timeout() -> None:
            if not task.done():
                if timeout_error and not result.done():
                    result.set_exception(timeout_error)

                task.cancel()
                result.cancel()

        handle = self.loop.call_later(timeout, set_timeout)
        try:
            return await result
        finally:
            handle.cancel()

    def async_as_background[T_Ret](
        self, coro: Coroutine[object, object, T_Ret], *, silent: bool = True
    ) -> asyncio.Task[T_Ret]:
        task = self.loop.create_task(coro)

        if silent:
            task.add_done_callback(self.tramp.silent_reporter)
        else:
            task.add_done_callback(self.tramp.reporter)

        return task

    def child(self, *, name: str) -> Self:
        final_future: asyncio.Future[None] = self.loop.create_future()
        self.tramp.set_future_name(final_future, name=f"FUT{{{name}}}")
        final_future.add_done_callback(self.tramp.silent_reporter)

        return dataclasses.replace(
            self,
            name=f"{self.name}-->{name}",
            tramp=self.tramp,
            futs=tuple([*self.futs, final_future]),
        )

    def __await__(self) -> Generator[None]:
        return self._wait().__await__()

    async def _wait(self) -> None:
        for fut in reversed(self.futs):
            if fut.done():
                await fut
                return

        await self.wait_for_all_futures(self)

        for fut in reversed(self.futs):
            if fut.done():
                await fut
                return

    __iter__ = __await__


if TYPE_CHECKING:
    _T: _protocols.Tramp = cast(Tramp, None)
    _CTX: _protocols.CTX[Tramp] = cast(CTX[Tramp], None)
    _WBC: _protocols.WaitByCallback[None] = cast(CTX[Tramp], None)
    _CB: _protocols.FutureCallback[None] = cast(_CTXCallback[None, Tramp], None)
