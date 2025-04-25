import asyncio
import dataclasses
import types
from collections.abc import Coroutine, Iterator
from typing import Self

from . import _async_mixin, _context, _protocols


@dataclasses.dataclass(frozen=True, kw_only=True)
class TaskHolder[T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    An object for managing asynchronous coroutines.

    Usage looks like:

    .. code-block:: python

        from machinery import helpers as hp


        ctx: hp.CTX = ...

        async def something():
            await asyncio.sleep(5)

        with ctx.child(name="TaskHolder") as ctx_taskholder:
            with hp.TaskHolder(ctx=ctx_taskholder) as ts:
                ts.add(something())
                ts.add(something())

    If you don't want to use the context manager, you can say:

    .. code-block:: python

        from machinery import helpers as hp


        ctx: hp.CTX = ...

        async def something():
            await asyncio.sleep(5)

        ctx_taskholder = ctx.child(name="TaskHolder")
        ts = hp.TaskHolder(ctx=ctx_taskholder)

        try:
            ts.add(something())
            ts.add(something())
        finally:
            await ts.finish()
            ctx.cancel()

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

    .. automethod:: add

    .. automethod:: add_task

    .. automethod:: finish
    """

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        return await self.finish(exc_type, exc, tb)

    async def __aenter__(self) -> Self:
        async with _async_mixin.ensure_aexit(self):
            return await self.start()

    ctx: _context.CTX[T_Tramp]
    ts: list[_protocols.WaitByCallback[object]] = dataclasses.field(
        default_factory=list, init=False
    )

    _cleaner: list[asyncio.Task[None]] = dataclasses.field(default_factory=list, init=False)
    _cleaner_waiter: asyncio.Event = dataclasses.field(default_factory=asyncio.Event, init=False)

    def add[T_Ret](
        self, coro: Coroutine[object, object, T_Ret], *, silent: bool = False
    ) -> asyncio.Task[T_Ret]:
        return self.add_task(self.ctx.async_as_background(coro, silent=silent))

    def _set_cleaner_waiter(self, res: _protocols.FutureStatus[object]) -> None:
        self._cleaner_waiter.set()

    def add_task[T_Ret](self, task: asyncio.Task[T_Ret]) -> asyncio.Task[T_Ret]:
        if not self._cleaner:
            t = self.ctx.async_as_background(self.cleaner())
            self._cleaner.append(t)

            def remove_cleaner(res: _protocols.FutureStatus[None]) -> None:
                if self._cleaner and self._cleaner[0] is t:
                    self._cleaner.clear()

            t.add_done_callback(remove_cleaner)

        task.add_done_callback(self._set_cleaner_waiter)
        self.ts.append(task)
        return task

    async def start(self) -> Self:
        return self

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        if exc and not self.ctx.done():
            self.ctx.set_exception(exc)

        try:
            while any(not t.done() for t in self.ts):
                for t in self.ts:
                    if self.ctx.done():
                        t.cancel()

                if self.ts:
                    if self.ctx.done():
                        await self.ctx.wait_for_all_futures(self.ctx, *self.ts)
                    else:
                        await self.ctx.wait_for_first_future(self.ctx, *self.ts)

                    self.ts[:] = [t for t in self.ts if not t.done()]
        finally:
            if self._cleaner:
                self._cleaner[0].cancel()
                await self.ctx.wait_for_all_futures(*self._cleaner)

            await self.ctx.wait_for_all_futures(self.ctx.async_as_background(self.clean()))

    @property
    def pending(self) -> int:
        return sum(1 for t in self.ts if not t.done())

    def __contains__(self, task: asyncio.Task[object]) -> bool:
        return task in self.ts

    def __iter__(self) -> Iterator[_protocols.WaitByCallback[object]]:
        return iter(self.ts)

    async def cleaner(self) -> None:
        while True:
            await self._cleaner_waiter.wait()
            self._cleaner_waiter.clear()
            await self.clean()

    async def clean(self) -> None:
        destroyed = []
        remaining = []
        for t in self.ts:
            if t.done():
                destroyed.append(t)
            else:
                remaining.append(t)

        await self.ctx.wait_for_all_futures(*destroyed)
        self.ts[:] = remaining + [t for t in self.ts if t not in destroyed and t not in remaining]
