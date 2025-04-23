import asyncio
import logging
import sys
from collections.abc import AsyncGenerator, Iterator
from unittest import mock

import pytest

from machinery import helpers as hp

log = logging.getLogger()


@pytest.fixture
def ctx() -> Iterator[hp.CTX]:
    tramp: hp.protocols.Tramp = hp.Tramp(log=log)
    with hp.CTX.beginning(name="::", tramp=tramp) as ctx:
        yield ctx


class TestStopAsyncGenerator:
    async def test_can_cancel_a_generator(self, loop: asyncio.AbstractEventLoop) -> None:
        called: list[object] = []
        ready = loop.create_future()

        async def d() -> AsyncGenerator[int]:
            try:
                called.append("wait")
                ready.set_result(True)
                yield 1
            except:
                called.append(sys.exc_info())
                raise
            finally:
                called.append("finally")

        gen = d()
        assert await gen.asend(None) == 1

        assert called == ["wait"]

        with pytest.raises(asyncio.CancelledError):
            await hp.stop_async_generator(gen)

        assert called == [
            "wait",
            (asyncio.CancelledError, mock.ANY, mock.ANY),
            "finally",
        ]

    async def test_can_throw_an_arbitrary_exception_into_the_generator(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        called: list[object] = []
        ready = loop.create_future()

        async def d() -> AsyncGenerator[int]:
            try:
                called.append("wait")
                ready.set_result(True)
                yield 1
            except:
                called.append(sys.exc_info())
                raise
            finally:
                called.append("finally")

        gen = d()
        assert await gen.asend(None) == 1

        assert called == ["wait"]

        error = ValueError("NOPE")
        with pytest.raises(ValueError, match="NOPE"):
            await hp.stop_async_generator(gen, exc=error)

        assert called == [
            "wait",
            (ValueError, error, mock.ANY),
            "finally",
        ]

    async def test_works_if_generator_is_already_complete(self) -> None:
        async def d() -> AsyncGenerator[bool]:
            yield True

        gen = d()
        async for _ in gen:
            pass

        await hp.stop_async_generator(gen)

    async def test_works_if_generator_is_already_complete_by_cancellation(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        async def d() -> AsyncGenerator[bool]:
            fut = loop.create_future()
            fut.cancel()
            await fut
            yield True

        gen = d()
        with pytest.raises(asyncio.CancelledError):
            async for _ in gen:
                pass

        await hp.stop_async_generator(gen)

    async def test_works_if_generator_is_already_complete_by_exception(self) -> None:
        async def d() -> AsyncGenerator[bool]:
            raise ValueError("NOPE")
            yield True

        gen = d()
        with pytest.raises(ValueError, match="NOPE"):
            async for _ in gen:
                pass

        await hp.stop_async_generator(gen)

    async def test_works_if_generator_is_half_complete(self) -> None:
        called: list[object] = []

        async def d() -> AsyncGenerator[int]:
            called.append("start")
            try:
                for i in range(10):
                    called.append(i)
                    yield i
            except asyncio.CancelledError:
                called.append("cancel")
                raise
            except:
                called.append(("except", sys.exc_info()))
                raise
            finally:
                called.append("finally")

        gen = d()
        async for i in gen:
            if i == 5:
                break

        assert called == ["start", 0, 1, 2, 3, 4, 5]

        with pytest.raises(asyncio.CancelledError):
            await hp.stop_async_generator(gen)
        assert called == ["start", 0, 1, 2, 3, 4, 5, "cancel", "finally"]

    async def test_works_if_generator_is_cancelled_inside(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        waiter = loop.create_future()

        called: list[object] = []

        async def d() -> AsyncGenerator[int]:
            called.append("start")
            try:
                for i in range(10):
                    if waiter.done():
                        await waiter
                    called.append(i)
                    yield i
            except asyncio.CancelledError:
                called.append("cancel")
                raise
            except:
                called.append(("except", sys.exc_info()))
                raise
            finally:
                called.append("finally")

        gen = d()

        with pytest.raises(asyncio.CancelledError):
            async for i in gen:
                if i == 5:
                    waiter.cancel()

        assert called == ["start", 0, 1, 2, 3, 4, 5, "cancel", "finally"]
        await hp.stop_async_generator(gen)

    async def test_works_if_generator_is_cancelled_outside(
        self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
    ) -> None:
        waiter = loop.create_future()

        called: list[object] = []

        async def d() -> AsyncGenerator[int]:
            called.append("start")
            try:
                for i in range(10):
                    if waiter.done():
                        await waiter
                    called.append(i)
                    yield i
            except asyncio.CancelledError:
                called.append("cancel")
                raise
            except:
                called.append(("except", sys.exc_info()))
                raise
            finally:
                called.append("finally")

        gen = d()

        async def consume() -> None:
            async for i in gen:
                if i == 5:
                    waiter.set_result(True)
                    await loop.create_future()

        with pytest.raises(asyncio.CancelledError):
            task = ctx.async_as_background(consume())
            await waiter
            task.cancel()
            await task

        assert called == ["start", 0, 1, 2, 3, 4, 5]
        with pytest.raises(asyncio.CancelledError):
            await hp.stop_async_generator(gen)
        assert called == ["start", 0, 1, 2, 3, 4, 5, "cancel", "finally"]
