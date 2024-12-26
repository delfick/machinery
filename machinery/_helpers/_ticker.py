import asyncio
import time
import types
from collections.abc import AsyncGenerator
from typing import Self

from . import _async_mixin, _futures, _task_holder


class ATicker:
    """
    This object gives you an async generator that yields every ``every``
    seconds, taking into account how long it takes for your code to finish
    for the next yield.

    For example:

    .. code-block:: python

        from machinery import helpers as hp

        import time


        start = time.time()
        timing = []

        async for _ in hp.ATicker(10):
            timing.append(time.time() - start)
            asyncio.sleep(8)
            if len(timing) >= 5:
                break

        assert timing == [0, 10, 20, 30, 40]

    The value that is yielded is a tuple of (iteration, time_till_next) where
    ``iteration`` is a counter of how many times we yield a value starting from
    1 and the ``time_till_next`` is the number of seconds till the next time we
    yield a value.

    You can use the shortcut :func:`tick` to create one of these, but if you
    do create this yourself, you can change the ``every`` value while you're
    iterating.

    .. code-block:: python

        from machinery import helpers as hp


        ticker = hp.ATicker(10)

        done = 0

        async with ticker as ticks:
            async for _ in ticks:
                done += 1
                if done == 3:
                    # This will mean the next tick will be 20 seconds after the last
                    # tick and future ticks will be 20 seconds apart
                    ticker.change_after(20)
                elif done == 5:
                    # This will mean the next tick will be 40 seconds after the last
                    # tick, but ticks after that will go back to 20 seconds apart.
                    ticker.change_after(40, set_new_every=False)

    There are three other options:

    final_future
        If this future is completed then the iteration will stop

    max_iterations
        Iterations after this number will cause the loop to finish. By default
        there is no limit

    max_time
        After this many iterations the loop will stop. By default there is no
        limit

    min_wait
        The minimum amount of time to wait after a tick.

        If this is False then we will always just tick at the next expected time,
        otherwise we ensure this amount of time at a minimum between ticks

    pauser
        If not None, we use this as a semaphore in an async with to pause the ticks
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

    class Stop(Exception):
        pass

    def __init__(
        self,
        every: int,
        *,
        final_future: asyncio.Future[object] | None = None,
        max_iterations: int | None = None,
        max_time: int | None = None,
        min_wait: float = 0.1,
        pauser: asyncio.Semaphore | None = None,
        name: str | None = None,
    ) -> None:
        self.name = name
        self.every = every
        self.pauser = pauser
        self.max_time = max_time
        self.min_wait = min_wait
        self.max_iterations = max_iterations

        if self.every <= 0:
            self.every = 0
            if self.min_wait is False:
                self.min_wait = 0

        self.handle: asyncio.Handle | None = None
        self.expected: float | None = None

        self.waiter = _futures.ResettableFuture(name=f"ATicker({self.name})::__init__[waiter]")
        self.final_future = _futures.ChildOfFuture(
            final_future
            or _futures.create_future(name=f"ATicker({self.name})::__init__[owned_final_future]"),
            name=f"ATicker({self.name})::__init__[final_future]",
        )

    async def start(self) -> Self:
        self.gen = self.tick()
        return self

    def __aiter__(self) -> AsyncGenerator[tuple[int, float], None]:
        if not hasattr(self, "gen"):
            raise Exception(
                "The ticker must be used as a context manager before being used as an async iterator"
            )
        return self.gen

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        if hasattr(self, "gen"):
            try:
                await _futures.stop_async_generator(
                    self.gen, exc=exc or self.Stop(), name=f"ATicker({self.name})::stop[stop_gen]"
                )
            except self.Stop:
                pass

        self.final_future.cancel()

    async def tick(self) -> AsyncGenerator[tuple[int, float], None]:
        final_handle = None
        if self.max_time:
            final_handle = _futures.get_event_loop().call_later(
                self.max_time, self.final_future.cancel
            )

        try:
            async for info in self._tick():
                yield info
        finally:
            self.final_future.cancel()
            if final_handle:
                final_handle.cancel()
            self._change_handle()

    def change_after(self, every: int, *, set_new_every: bool = True) -> None:
        old_every = self.every
        if set_new_every:
            self.every = every

        if self.expected is None:
            return

        last = self.expected - old_every

        expected = last + every
        if set_new_every:
            self.expected = expected

        diff = round(expected - time.time(), 3)
        self._change_handle()

        if diff <= 0:
            self.waiter.reset()
            self.waiter.set_result(True)
        else:
            self._change_handle(_futures.get_event_loop().call_later(diff, self._waited))

    def _change_handle(self, handle: asyncio.Handle | None = None) -> None:
        if self.handle:
            self.handle.cancel()
        self.handle = handle

    def _waited(self) -> None:
        self.waiter.reset()
        self.waiter.set_result(True)

    async def _wait_for_next(self) -> None:
        pauser = self.pauser

        if pauser is None or not pauser.locked():
            return await _futures.wait_for_first_future(
                self.final_future,
                self.waiter,
                name=f"ATicker({self.name})::_wait_for_next[without_pause]",
            )

        async def pause() -> None:
            async with pauser:
                pass

        ts_final_future = _futures.ChildOfFuture(
            self.final_future, name=f"ATicker({self.name})::_wait_for_next[with_pause]"
        )

        async with _task_holder.TaskHolder(ts_final_future) as ts:
            ts.add(pause())
            ts.add_task(self.waiter)

    async def _tick(self) -> AsyncGenerator[tuple[int, float], None]:
        start = time.time()
        iteration = 0
        self.expected = start

        self._waited()

        while True:
            await self._wait_for_next()

            self.waiter.reset()
            if self.final_future.done():
                return

            if self.max_iterations is not None and iteration >= self.max_iterations:
                return

            now = time.time()
            if self.max_time is not None and now - start >= self.max_time:
                return

            if self.min_wait is False:
                diff = self.expected - now
                if diff == 0:
                    self.expected += self.every
                else:
                    while diff <= -self.every:
                        self.expected += self.every
                        diff = self.expected - now

                    while self.expected - now <= 0:
                        self.expected += self.every
            else:
                diff = self.min_wait
                if self.every > 0:
                    while self.expected - now < self.min_wait:
                        self.expected += self.every

                    diff = round(self.expected - now, 3)

            if diff == 0:
                diff = self.expected - now

            self._change_handle(_futures.get_event_loop().call_later(diff, self._waited))

            if self.min_wait is not False or diff > 0:
                iteration += 1
                yield iteration, max([diff, 0])


def tick(
    every: int,
    *,
    final_future: asyncio.Future[object] | None = None,
    max_iterations: int | None = None,
    max_time: int | None = None,
    min_wait: float = 0.1,
    name: str | None = None,
    pauser: asyncio.Semaphore | None = None,
) -> ATicker:
    """
    .. code-block:: python

        from machinery import helpers as hp


        async with hp.tick(every) as ticks:
            async for i in ticks:
                yield i

        # Is a nicer way of saying

        async for i in hp.ATicker(every):
            yield i

    If you want control of the ticker during the iteration, then use
    :class:`ATicker` directly.
    """
    return ATicker(
        every,
        final_future=final_future,
        max_iterations=max_iterations,
        max_time=max_time,
        min_wait=min_wait,
        pauser=pauser,
        name=f"||tick({name})",
    )
