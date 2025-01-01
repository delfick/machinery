import types
from typing import Self

from . import _async_mixin, _future_waiters, _future_wrappers, _futures


class TaskHolder:
    """
    An object for managing asynchronous coroutines.

    Usage looks like:

    .. code-block:: python

        from machinery import helpers as hp


        final_future = hp.create_future()

        async def something():
            await asyncio.sleep(5)

        with hp.TaskHolder(final_future) as ts:
            ts.add(something())
            ts.add(something())

    If you don't want to use the context manager, you can say:

    .. code-block:: python

        from machinery import helpers as hp


        final_future = hp.create_future()

        async def something():
            await asyncio.sleep(5)

        ts = hp.TaskHolder(final_future)

        try:
            ts.add(something())
            ts.add(something())
        finally:
            await ts.finish()

    Once your block in the context manager is done the context manager won't
    exit until all coroutines have finished. During this time you may still
    use ``ts.add`` or ``ts.add_task`` on the holder.

    If the ``final_future`` is cancelled before all the tasks have completed
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

    def __init__(self, final_future, *, name=None):
        self.name = name

        self.ts = []
        self.final_future = _future_wrappers.ChildOfFuture(
            final_future, name=f"TaskHolder({self.name})::__init__[final_future]"
        )

        self._cleaner = None
        self._cleaner_waiter = _future_wrappers.ResettableFuture(
            name=f"TaskHolder({self.name})::__init__[cleaner_waiter]"
        )

    def add(self, coro, *, silent=False):
        return self.add_task(_futures.async_as_background(coro, silent=silent))

    def _set_cleaner_waiter(self, res):
        self._cleaner_waiter.reset()
        self._cleaner_waiter.set_result(True)

    def add_task(self, task):
        if not self._cleaner:
            self._cleaner = _futures.async_as_background(self.cleaner())

            t = self._cleaner

            def remove_cleaner(res):
                if self._cleaner is t:
                    self._cleaner = None

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
        if exc and not self.final_future.done():
            self.final_future.set_exception(exc)

        try:
            while any(not t.done() for t in self.ts):
                for t in self.ts:
                    if self.final_future.done():
                        t.cancel()

                if self.ts:
                    if self.final_future.done():
                        await _future_waiters.wait_for_all_futures(
                            self.final_future,
                            *self.ts,
                            name=f"TaskHolder({self.name})::finish[wait_for_all_tasks]",
                        )
                    else:
                        await _future_waiters.wait_for_first_future(
                            self.final_future,
                            *self.ts,
                            name=f"TaskHolder({self.name})::finish[wait_for_another_task]",
                        )

                    self.ts = [t for t in self.ts if not t.done()]
        finally:
            try:
                await self._final()
            finally:
                self.final_future.cancel()

    async def _final(self):
        if self._cleaner:
            self._cleaner.cancel()
            await _future_waiters.wait_for_all_futures(
                self._cleaner, name=f"TaskHolder({self.name})::finish[finally_wait_for_cleaner]"
            )

        await _future_waiters.wait_for_all_futures(
            _futures.async_as_background(self.clean()),
            name=f"TaskHolder({self.name})::finish[finally_wait_for_clean]",
        )

    @property
    def pending(self):
        return sum(1 for t in self.ts if not t.done())

    def __contains__(self, task):
        return task in self.ts

    def __iter__(self):
        return iter(self.ts)

    async def cleaner(self):
        while True:
            await self._cleaner_waiter
            self._cleaner_waiter.reset()
            await self.clean()

    async def clean(self):
        destroyed = []
        remaining = []
        for t in self.ts:
            if t.done():
                destroyed.append(t)
            else:
                remaining.append(t)

        await _future_waiters.wait_for_all_futures(
            *destroyed, name=f"TaskHolder({self.name})::clean[wait_for_destroyed]"
        )
        self.ts = remaining + [t for t in self.ts if t not in destroyed and t not in remaining]
