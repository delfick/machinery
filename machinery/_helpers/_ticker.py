import asyncio
import contextlib
import dataclasses
import sys
import time
from collections.abc import AsyncGenerator

from . import _futures, _protocols


class _Stop(Exception):
    pass


@dataclasses.dataclass(kw_only=True)
class _TickerSchedule:
    """
    A mutable class that holds mutable values. This allows us to set
    everything else to frozen dataclasses
    """

    every: int
    handle: asyncio.Handle | None = None
    expected: float | None = None


@dataclasses.dataclass(frozen=True, kw_only=True)
class _TickerOptions[T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    Represents the logic held by the ticker. It is used by the ``tick`` function
    below to implement the desired behaviour.
    """

    ctx: _protocols.CTX[T_Tramp]
    schedule: _TickerSchedule
    max_time_reached: _protocols.WaitByCallback[None]

    max_iterations: int | None = None
    max_time: int | None = None
    min_wait: float = 0.1
    pauser: asyncio.Semaphore | None = None

    waiter: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)

    def change_after(self, every: int, *, set_new_every: bool = True) -> None:
        """
        Used to change the schedule for the ticker.

        If ``set_new_every`` is False, then this new schedule is once only.
        """
        old_every = self.schedule.every
        if set_new_every:
            self.schedule.every = every

        if self.schedule.expected is None:
            return

        last = self.schedule.expected - old_every

        expected = last + every
        if set_new_every:
            self.schedule.expected = expected

        diff = round(expected - time.time(), 3)
        self._change_handle()

        if diff <= 0:
            self.waiter.set()
        else:
            self._change_handle(self.ctx.loop.call_later(diff, self.waiter.set))

    async def tick(self) -> AsyncGenerator[tuple[int, float]]:
        """
        The main loop for the ticker. It will yield as per the schedule and take
        into account when the schedule changes.
        """
        start = time.time()
        iteration = 0
        self.schedule.expected = start

        # We yield before the first schedule
        self.waiter.set()

        while True:
            await self._wait_for_next()

            self.waiter.clear()
            if self.ctx.done():
                return

            if self.max_time_reached.done():
                return

            if self.max_iterations is not None and iteration >= self.max_iterations:
                return

            now = time.time()
            if self.max_time is not None and now - start >= self.max_time:
                return

            # We do some mildly complicated magic to work out how long to wait
            # for the next time we yield some values
            if self.min_wait is False:
                diff = self.schedule.expected - now
                if diff == 0:
                    self.schedule.expected += self.schedule.every
                else:
                    while diff <= -self.schedule.every:
                        self.schedule.expected += self.schedule.every
                        diff = self.schedule.expected - now

                    while self.schedule.expected - now <= 0:
                        self.schedule.expected += self.schedule.every
            else:
                diff = self.min_wait
                if self.schedule.every > 0:
                    while self.schedule.expected - now < self.min_wait:
                        self.schedule.expected += self.schedule.every

                    diff = round(self.schedule.expected - now, 3)

            if diff == 0:
                diff = self.schedule.expected - now

            # We ensure we only ever have the latest waiter setter at a time
            # This allows us to the change the schedule and not be Interrupted
            # By the old schedule
            self._change_handle(self.ctx.loop.call_later(diff, self.waiter.set))

            if self.min_wait is not False or diff > 0:
                iteration += 1
                yield iteration, max([diff, 0])

    def _change_handle(self, handle: asyncio.Handle | None = None) -> None:
        """
        Used to ensure we only have one callback to call to wake up the tick.
        """
        if self.schedule.handle:
            self.schedule.handle.cancel()
        self.schedule.handle = handle

    async def _wait_for_next(self) -> None:
        """
        Used to wake up the tick the next time it should

        This takes into account the pauser semaphore and ensures that it is unlocked
        when we wake up next.
        """
        pauser = self.pauser

        if pauser is not None and pauser.locked():

            async def pause() -> None:
                async with pauser:
                    pass

            task = self.ctx.async_as_background(pause())
            try:
                await self.ctx.wait_for_first(task, self.ctx)
            finally:
                task.cancel()
                await self.ctx.wait_for_all(task)

        return await self.ctx.wait_for_first(self.ctx, self.waiter, self.max_time_reached)


@dataclasses.dataclass(frozen=True, kw_only=True)
class _Ticker[T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    Object used to represent the Ticker protocol that is yielded from the
    tick helper below.
    """

    _options: _TickerOptions[T_Tramp]
    _gen: AsyncGenerator[tuple[int, float]]

    @property
    def pauser(self) -> asyncio.Semaphore | None:
        """
        The semaphore that lets the user pause the tick.
        """
        return self._options.pauser

    def __aiter__(self) -> AsyncGenerator[tuple[int, float]]:
        """
        The async generator that yields values at the desired schedule.
        """
        return self._gen

    def change_after(self, every: int, *, set_new_every: bool = True) -> None:
        """
        Used to change the schedule.
        """
        self._options.change_after(every, set_new_every=set_new_every)


@contextlib.asynccontextmanager
async def tick[T_Tramp: _protocols.Tramp = _protocols.Tramp](
    every: int,
    *,
    ctx: _protocols.CTX[T_Tramp],
    max_iterations: int | None = None,
    max_time: int | None = None,
    min_wait: float = 0.1,
    name: str = "",
    pauser: asyncio.Semaphore | None = None,
) -> AsyncGenerator[_protocols.Ticker]:
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

        ctx: hp.CTX = ...

        async with hp.tick(10, ctx=ctx) as ticker:
            async for _ in ticker:
                timing.append(time.time() - start)
                asyncio.sleep(8)
                if len(timing) >= 5:
                    break

        assert timing == [0, 10, 20, 30, 40]

    The value that is yielded is a tuple of (iteration, time_till_next) where
    ``iteration`` is a counter of how many times we yield a value starting from
    1 and the ``time_till_next`` is the number of seconds till the next time we
    yield a value.

    Note that the schedule value can be changed during iteration:

    .. code-block:: python

        from machinery import helpers as hp

        ctx: hp.CTX = ...

        async with hp.tick(10, ctx=ctx) as ticker:
            done = 0

            async for _ in ticker:
                done += 1
                if done == 3:
                    # This will mean the next tick will be 20 seconds after the last
                    # tick and future ticks will be 20 seconds apart
                    ticker.change_after(20)
                elif done == 5:
                    # This will mean the next tick will be 40 seconds after the last
                    # tick, but ticks after that will go back to 20 seconds apart.
                    ticker.change_after(40, set_new_every=False)

    There are other options:

    ctx
        If this ctx is completed then the iteration will stop

    max_iterations
        Iterations after this number will cause the loop to finish. By default
        there is no limit

    max_time
        After this many iterations the loop will stop. By default there is no
        limit

    min_wait
        The minimum amount of time to wait after a tick.

        If this is False then we will always tick at the next expected time,
        otherwise we ensure this amount of time at a minimum between ticks

    pauser
        If not None, we use this as a semaphore that will pause the ticks when
        it is locked.
    """
    with ctx.child(name="{name}ticker", prefix=name) as ctx_ticker:
        max_time_reached = ctx.loop.create_future()
        ctx.tramp.set_future_name(max_time_reached, name=f"{ctx_ticker.name}::[max_time_reached]")

        def ensure_max_time_cancelled(res: _protocols.FutureStatus[None]) -> None:
            max_time_reached.cancel()

        ctx_ticker.add_done_callback(ensure_max_time_cancelled)

        if max_time:
            ctx.loop.call_later(max_time, max_time_reached.cancel)

        options = _TickerOptions(
            schedule=_TickerSchedule(every=every),
            ctx=ctx_ticker,
            max_time_reached=max_time_reached,
            max_iterations=max_iterations,
            max_time=max_time,
            min_wait=min_wait,
            pauser=pauser,
        )

        gen = options.tick()
        try:
            yield _Ticker(_options=options, _gen=gen)
        finally:
            # Make sure the ticker is cleaned up
            exc_info = sys.exc_info()
            try:
                await _futures.stop_async_generator(gen, exc=exc_info[1] or _Stop())
            except _Stop:
                pass
