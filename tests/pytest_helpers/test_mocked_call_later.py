import asyncio
import time

import pytest

from machinery import test_helpers as thp


@pytest.fixture()
def loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_event_loop_policy().get_event_loop()


class TestMockedCalledLater:
    async def test_works(self, loop: asyncio.AbstractEventLoop):
        with thp.FakeTime() as t:
            async with thp.MockedCallLater(t, loop=loop):
                waiter = asyncio.Event()
                loop.call_later(5, waiter.set)
                await waiter.wait()
                assert time.time() == 5

    async def test_does_the_calls_in_order(self, loop: asyncio.AbstractEventLoop):
        with thp.FakeTime() as t:
            async with thp.MockedCallLater(t, loop=loop):
                assert time.time() == 0

                called = []
                waiter = asyncio.Event()

                def c(v):
                    called.append((time.time(), v))
                    if len(called) == 4:
                        waiter.set()

                loop.call_later(2, c, "2")
                loop.call_later(1, c, "1")
                loop.call_later(5, c, "5")
                loop.call_later(0.3, c, "0.3")

                await waiter.wait()

                assert called == [(0.3, "0.3"), (1, "1"), (2, "2"), (5, "5")]

    async def test_can_cancel_handles(self, loop: asyncio.AbstractEventLoop):
        with thp.FakeTime() as t:
            async with thp.MockedCallLater(t, loop=loop) as m:
                info = {"handle": None}

                def nxt(*args):
                    if info["handle"]:
                        info["handle"].cancel()

                    info["handle"] = loop.call_later(*args)

                waiter = asyncio.Event()
                nxt(1, waiter.set)
                nxt(0.3, waiter.set)

                await waiter.wait()
                waiter.clear()
                assert time.time() == 0.3

                await m.add(1)
                assert time.time() == 1.3
                await waiter.wait()
                waiter.clear()

                nxt(2, waiter.set)
                await m.add(1.5)
                assert time.time() == 2.8

                nxt(1.5, waiter.set)
                await m.add(0.6)
                assert time.time() == 3.4
                assert not waiter.is_set()

                await waiter.wait()
                assert time.time() == 2.8 + 1.5
                assert time.time() == 0.3 + 1 + 1.5 + 1.5

                waiter.clear()
                nxt(0.3, waiter.set)
                await m.add(0.4)
                await waiter.wait()

                assert time.time() == 0.3 + 1 + 1.5 + 1.5 + 0.4
