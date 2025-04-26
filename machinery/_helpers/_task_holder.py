import asyncio
import contextlib
import dataclasses
import types
from collections.abc import AsyncGenerator, Coroutine, Iterator
from typing import Self

from . import _async_mixin, _protocols


@dataclasses.dataclass(frozen=True, kw_only=True)
class _TaskHolder[T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        return await self._finish(exc_type, exc, tb)

    async def __aenter__(self) -> Self:
        async with _async_mixin.ensure_aexit(self):
            return await self._start()

    _ctx: _protocols.CTX[T_Tramp]
    _ts: list[_protocols.WaitByCallback[object]] = dataclasses.field(
        default_factory=list, init=False
    )

    _cleaner: list[asyncio.Task[None]] = dataclasses.field(default_factory=list, init=False)
    _cleaner_waiter: asyncio.Event = dataclasses.field(default_factory=asyncio.Event, init=False)

    def add_coroutine[T_Ret](
        self, coro: Coroutine[object, object, T_Ret], *, silent: bool = False
    ) -> asyncio.Task[T_Ret]:
        return self.add_task(self._ctx.async_as_background(coro, silent=silent))

    def _set_cleaner_waiter(self, res: _protocols.FutureStatus[object]) -> None:
        self._cleaner_waiter.set()

    def add_task[T_Ret](self, task: asyncio.Task[T_Ret]) -> asyncio.Task[T_Ret]:
        if not self._cleaner:
            t = self._ctx.async_as_background(self._cleaner_task())
            self._cleaner.append(t)

            def remove_cleaner(res: _protocols.FutureStatus[None]) -> None:
                if self._cleaner and self._cleaner[0] is t:
                    self._cleaner.clear()

            t.add_done_callback(remove_cleaner)

        task.add_done_callback(self._set_cleaner_waiter)
        self._ts.append(task)
        return task

    async def _start(self) -> Self:
        return self

    async def _finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        if exc and not self._ctx.done():
            self._ctx.set_exception(exc)

        try:
            while any(not t.done() for t in self._ts):
                for t in self._ts:
                    if self._ctx.done():
                        t.cancel()

                if self._ts:
                    if self._ctx.done():
                        await self._ctx.wait_for_all(self._ctx, *self._ts)
                    else:
                        await self._ctx.wait_for_first(self._ctx, *self._ts)

                    self._ts[:] = [t for t in self._ts if not t.done()]
        finally:
            if self._cleaner:
                self._cleaner[0].cancel()
                await self._ctx.wait_for_all(*self._cleaner)

            await self._ctx.wait_for_all(self._ctx.async_as_background(self._perform_clean()))

    @property
    def pending(self) -> int:
        return sum(1 for t in self._ts if not t.done())

    def __contains__(self, task: asyncio.Task[object]) -> bool:
        return task in self._ts

    def __iter__(self) -> Iterator[_protocols.WaitByCallback[object]]:
        return iter(self._ts)

    async def _cleaner_task(self) -> None:
        while True:
            await self._cleaner_waiter.wait()
            self._cleaner_waiter.clear()
            await self._perform_clean()

    async def _perform_clean(self) -> None:
        destroyed = []
        remaining = []
        for t in self._ts:
            if t.done():
                destroyed.append(t)
            else:
                remaining.append(t)

        await self._ctx.wait_for_all(*destroyed)
        self._ts[:] = remaining + [
            t for t in self._ts if t not in destroyed and t not in remaining
        ]


@contextlib.asynccontextmanager
async def task_holder(
    *, ctx: _protocols.CTX, name: str = ""
) -> AsyncGenerator[_protocols.TaskHolder]:
    """
    An object for managing asynchronous coroutines.

    Usage looks like:

    .. code-block:: python

        from machinery import helpers as hp


        async def my_async_program(ctx: hp.CTX) -> None:
            async def something():
                await asyncio.sleep(5)

            async with hp.task_holder(ctx=ctx) as ts:
                ts.add_coroutine(something())
                ts.add_coroutine(something())

    If you don't want to use the context manager, you can say:

    .. code-block:: python

        from machinery import helpers as hp
        import contextlib


        async def something():
            await asyncio.sleep(5)

        async def my_async_program(ctx: hp.CTX) -> None:
            exit_stack = contextlib.AsyncExitStack()

            ts = await exit_stack.enter_async_context(hp.task_holder(ctx=ctx))

            try:
                ts.add_coroutine(something())
                ts.add_coroutine(something())
            finally:
                await exit_stack.aclose()

    Once your block in the context manager is done the context manager won't
    exit until all coroutines have finished. During this time you may still
    use ``ts.add`` or ``ts.add_task`` on the holder.

    If the ``ctx`` is cancelled before all the tasks have completed
    then the tasks will be cancelled and properly waited on so their finally
    blocks run before the context manager finishes.

    ``ts.add`` will also return the task object that is made from the coroutine.

    ``ts.add`` also takes a ``silent=False`` parameter, that when True will
    not log any errors that happen. Otherwise errors will be logged.

    If you already have a task object, you can give it to the holder with
    ``ts.add_task(my_task)``.
    """

    with ctx.child(name=f"{name}task_holder", prefix=name) as ctx_task_holder:
        async with _TaskHolder(_ctx=ctx_task_holder) as task_holder:
            yield task_holder
