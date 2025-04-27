import asyncio
import contextlib
import dataclasses
import types
from collections.abc import AsyncGenerator, Coroutine, Iterator
from typing import Self

from . import _async_mixin, _protocols


@dataclasses.dataclass(frozen=True, kw_only=True)
class _TaskHolder[T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    Used to create and hold onto asyncio.Task objects to ensure they are
    cleaned up correctly to avoid asyncio complaining about task objects that
    are not awaited before the program ends.
    """

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        value: BaseException | None,
        tb: types.TracebackType | None,
    ) -> None:
        return await self._finish(exc_type, value, tb)

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
        """
        Create a task from the provided coroutine and register it.
        """
        return self.add_task(self._ctx.async_as_background(coro, silent=silent))

    def add_task[T_Ret](self, task: asyncio.Task[T_Ret]) -> asyncio.Task[T_Ret]:
        """
        Hold onto a task.

        We set an internal event when the task is done to ensure that we cleanup
        tasks as tasks are completed. This allows the task holder to be long
        lived and not hold onto ever task that it ends up knowing about and
        create an effective memory leak.
        """
        if not self._cleaner:
            # Ensure we have a task that cleans up done tasks
            t = self._ctx.async_as_background(self._cleaner_task())
            self._cleaner.append(t)

        # Ensure we wake up our cleanup task the next time a task completes
        task.add_done_callback(self._set_cleaner_waiter)

        self._ts.append(task)
        return task

    def _set_cleaner_waiter(self, res: _protocols.FutureStatus[object]) -> None:
        """
        A done callback to set the cleaner waiter that has a return annotation
        that aligns with task.add_done_callback

        This wakes up our cleanup task when a task finishes so that a cleanup
        is performed.
        """
        self._cleaner_waiter.set()

    async def _start(self) -> Self:
        """
        Nothing to do when entering the async context manager for this object
        """
        return self

    async def _finish(
        self,
        exc_typ: type[BaseException] | None = None,
        value: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        """
        When leaving the async context manager for this object, wait for all
        tasks to complete.

        If we leave the context manager with an exception or our ctx is done, then
        cancel all the tasks.

        Whilst this runs, we can still add more tasks to the holder.
        """
        if value and not self._ctx.done():
            self._ctx.set_exception(value)

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
                for cleaner in self._cleaner:
                    cleaner.cancel()
                await self._ctx.wait_for_all(*self._cleaner)

            await self._perform_clean()

    @property
    def pending(self) -> int:
        """
        Return the number of tasks held onto by this holder that are not done.
        """
        return sum(1 for t in self._ts if not t.done())

    def __contains__(self, task: asyncio.Task[object]) -> bool:
        """
        Return whether we are holding onto this task
        """
        return task in self._ts

    def __iter__(self) -> Iterator[_protocols.WaitByCallback[object]]:
        """
        Yield the tasks currently held onto by this holder
        """
        return iter(self._ts)

    async def _cleaner_task(self) -> None:
        """
        We ensure this function is running when we add new tasks to the holder.

        It ensures that tasks are cleaned up as they are completed so long
        lived task holders do not hold onto tasks indefinitely.

        It sits idle until tasks are completed.
        """
        while True:
            await self._cleaner_waiter.wait()
            self._cleaner_waiter.clear()
            await self._perform_clean()

    async def _perform_clean(self) -> None:
        """
        Cleanup any tasks this holder knows of that are now complete.

        This allows us to ensure that we don't hold onto tasks indefinitely
        if the task holder is long lived
        """
        destroyed = []
        remaining = []
        for t in self._ts:
            if t.done():
                destroyed.append(t)
            else:
                remaining.append(t)

        await self._ctx.wait_for_all(*destroyed)

        # Ensure we get both remaining tasks and any that have been added
        # While the cleanup was performed
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

    ``ts.add_coroutine`` will also return the task object that is made from the
    coroutine.

    ``ts.add_coroutine`` also takes a ``silent=False`` parameter, that when True
    will not log any errors that happen. Otherwise errors will be logged.

    If you already have a task object, you can give it to the holder with
    ``ts.add_task(my_task)``.
    """

    with ctx.child(name=f"{name}task_holder", prefix=name) as ctx_task_holder:
        async with _TaskHolder(_ctx=ctx_task_holder) as task_holder:
            yield task_holder
