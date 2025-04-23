import asyncio
import logging
import time
from collections.abc import AsyncGenerator, Iterator

import pytest

from machinery import helpers as hp
from machinery import test_helpers as thp

log = logging.getLogger()


@pytest.fixture
def ctx() -> Iterator[hp.CTX]:
    tramp: hp.protocols.Tramp = hp.Tramp(log=log)
    with hp.CTX.beginning(name="::", tramp=tramp) as ctx:
        yield ctx


@pytest.fixture
def fake_time() -> Iterator[thp.FakeTime]:
    with thp.FakeTime() as t:
        yield t


@pytest.fixture
async def fake_mocked_later(
    fake_time: thp.FakeTime, loop: asyncio.AbstractEventLoop
) -> AsyncGenerator[thp.MockedCallLater]:
    async with thp.MockedCallLater(fake_time, loop=loop) as m:
        yield m


class TestTick:
    async def test_keeps_yielding_such_that_yields_are_every_apart(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.tick(3, ctx=ctx) as ticks:
            async for i, nxt in ticks:
                called.append((i, nxt, time.time()))
                if len(called) == 5:
                    break

        assert called == [(1, 3, 0), (2, 3, 3), (3, 3, 6), (4, 3, 9), (5, 3, 12)]
        assert fake_mocked_later.called_times == [3, 6, 9, 12]

    async def test_works_with_0_every(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.tick(0, ctx=ctx, min_wait=0) as ticks:
            async for i, nxt in ticks:
                called.append((i, nxt, time.time()))
                if i == 3:
                    await fake_mocked_later.add(2)
                elif i == 6:
                    break

        assert called == [(1, 0, 0), (2, 0, 0), (3, 0, 0), (4, 0, 2.0), (5, 0, 2.0), (6, 0, 2.0)]

    async def test_works_with_0_every_and_nonzero_min_wait(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.tick(0, ctx=ctx, min_wait=0.1) as ticks:
            async for i, nxt in ticks:
                called.append((i, nxt, time.time()))
                if i == 3:
                    await fake_mocked_later.add(2)
                elif i == 6:
                    break

        assert called == [
            (1, 0.1, 0),
            (2, 0.1, 0.1),
            (3, 0.1, 0.2),
            (4, 0.1, 2.2),
            (5, 0.1, 2.3),
            (6, 0.1, 2.4),
        ]

    async def test_keeps_yielding_until_max_iterations(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.tick(3, ctx=ctx, max_iterations=5) as ticks:
            async for i, _ in ticks:
                called.append(i)

        assert called == [1, 2, 3, 4, 5]

    async def test_keeps_yielding_until_max_time(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.tick(3, ctx=ctx, max_time=20) as ticks:
            async for i, _ in ticks:
                called.append((i, time.time()))

        assert called == [(1, 0), (2, 3), (3, 6), (4, 9), (5, 12), (6, 15), (7, 18)]

    async def test_keeps_yielding_until_max_time_or_max_iterations(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.tick(3, ctx=ctx, max_iterations=5, max_time=20) as ticks:
            async for i, _ in ticks:
                called.append((i, time.time()))

        assert called == [(1, 0), (2, 3), (3, 6), (4, 9), (5, 12)]
        called.clear()

        async with hp.tick(3, ctx=ctx, max_iterations=10, max_time=20) as ticks:
            async for i, _ in ticks:
                called.append((i, time.time()))

        assert called == [
            (1, 15),
            (2, 18),
            (3, 21),
            (4, 24),
            (5, 27),
            (6, 30),
            (7, 33),
        ]

    async def test_keeps_yielding_such_that_yields_are_best_effort_every_apart_when_tasks_go_over(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ):
        called = []

        async with hp.tick(3, ctx=ctx) as ticks:
            async for i, _ in ticks:
                called.append((i, time.time()))

                await fake_mocked_later.add(2)

                if len(called) == 3:
                    await fake_mocked_later.add(3)

                if len(called) == 5:
                    await fake_mocked_later.add(7)

                if len(called) == 7:
                    break

        #                     0       3       6        9       12       15       18
        assert called == [(1, 0), (2, 3), (3, 6), (4, 11), (5, 13), (6, 22), (7, 24)]
        assert fake_mocked_later.called_times == [3, 6, 9, 12, 15, 24]

    async def test_stops_if_ctx_stops(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.tick(3, ctx=ctx) as ticks:
            async for _ in ticks:
                called.append(time.time())
                if len(called) == 5:
                    ctx.cancel()

        assert called == [0, 3, 6, 9, 12]
        assert fake_mocked_later.called_times == [3, 6, 9, 12]


class TestTicker:
    async def test_can_change_the_after_permanently(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.Ticker(3, ctx=ctx) as ticks:
            async for i, nxt in ticks:
                called.append((i, nxt, time.time()))

                if len(called) == 3:
                    ticks.change_after(5)

                elif len(called) == 5:
                    await fake_mocked_later.add(8)

                elif len(called) == 7:
                    await fake_mocked_later.add(1)

                elif len(called) == 10:
                    break

        assert called == [
            (1, 3, 0),
            (2, 3, 3),
            (3, 3, 6),
            (4, 5, 11),
            (5, 5, 16),
            (6, 2, 24),
            (7, 5, 26),
            (8, 5, 31),
            (9, 5, 36),
            (10, 5, 41),
        ]
        assert fake_mocked_later.called_times == [3, 6, 11, 16, 21, 26, 31, 36, 41]

    async def test_can_change_the_after_once(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.Ticker(3, ctx=ctx) as ticks:
            async for i, nxt in ticks:
                called.append((i, nxt, time.time()))

                if len(called) == 3:
                    ticks.change_after(5, set_new_every=False)

                elif len(called) == 6:
                    break

        assert called == [(1, 3, 0), (2, 3, 3), (3, 3, 6), (4, 1, 11), (5, 3, 12), (6, 3, 15)]
        assert fake_mocked_later.called_times == [3, 6, 11, 12, 15]

    async def test_can_have_a_minimum_wait(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.Ticker(5, ctx=ctx, min_wait=2) as ticks:
            async for i, nxt in ticks:
                called.append((i, nxt, time.time()))

                if len(called) == 2:
                    await fake_mocked_later.add(9)

                elif len(called) == 4:
                    break

        assert called == [(1, 5, 0), (2, 5, 5), (3, 6, 14), (4, 5, 20)]
        assert fake_mocked_later.called_times == [5, 10, 20]

    async def test_can_be_told_to_follow_the_schedule(
        self, fake_mocked_later: thp.MockedCallLater, ctx: hp.CTX
    ) -> None:
        called = []

        async with hp.Ticker(5, ctx=ctx, min_wait=False) as ticks:
            async for i, nxt in ticks:
                called.append((i, nxt, time.time()))

                if len(called) == 2:
                    await fake_mocked_later.add(9)

                elif len(called) == 4:
                    await fake_mocked_later.add(12)

                elif len(called) == 6:
                    break

        assert called == [
            (1, 5, 0),
            (2, 5.0, 5.0),
            (3, 1.0, 14.0),
            (4, 5.0, 15.0),
            (5, 3.0, 27.0),
            (6, 5.0, 30.0),
        ]
        assert fake_mocked_later.called_times == [5, 10, 14, 15, 20, 27, 30]

    class TestWithAPauser:
        async def test_can_be_paused(
            self,
            fake_mocked_later: thp.MockedCallLater,
            ctx: hp.CTX,
            loop: asyncio.AbstractEventLoop,
        ) -> None:
            called = []

            pauser = asyncio.Semaphore()

            async with hp.Ticker(5, ctx=ctx, min_wait=False, pauser=pauser) as ticks:
                async for i, nxt in ticks:
                    called.append((i, nxt, time.time()))

                    if len(called) == 2:
                        await pauser.acquire()
                        loop.call_later(28, pauser.release)

                    elif len(called) == 6:
                        break

            assert called == [
                (1, 5, 0),
                (2, 5.0, 5.0),
                (3, 2.0, 33.0),
                (4, 5.0, 35.0),
                (5, 5.0, 40.0),
                (6, 5.0, 45.0),
            ]
            assert fake_mocked_later.called_times == [5.0, 10.0, 33.0, 33.0, 35.0, 40.0, 45.0]

        async def test_cancelled_ctx_not_stopped_by_pauser(
            self,
            fake_mocked_later: thp.MockedCallLater,
            ctx: hp.CTX,
            loop: asyncio.AbstractEventLoop,
        ) -> None:
            called = []

            pauser = asyncio.Semaphore()

            async with hp.Ticker(5, ctx=ctx, min_wait=False, pauser=pauser) as ticks:
                async for i, nxt in ticks:
                    called.append((i, nxt, time.time()))

                    if len(called) == 2:
                        await pauser.acquire()
                        loop.call_later(14, ticks.ctx.cancel)

            assert time.time() == 19

            assert pauser.locked()
            assert called == [
                (1, 5, 0),
                (2, 5.0, 5.0),
            ]
            assert fake_mocked_later.called_times == [5.0, 10.0, 19.0]
