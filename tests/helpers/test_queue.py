import asyncio
import logging
from collections import deque
from collections.abc import Iterator
from queue import Queue as NormalQueue

import pytest

from machinery import helpers as hp
from machinery._helpers import queue as _queue


@pytest.fixture
def ctx() -> Iterator[hp.CTX]:
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    tramp: hp.protocols.Tramp = hp.Tramp(log=log)
    with hp.CTX.beginning(name="::", tramp=tramp) as ctx:
        yield ctx


class TestQueue:
    def test_takes_in_a_ctx(self, ctx: hp.CTX) -> None:
        queue = hp.queue(ctx=ctx)
        assert isinstance(queue, _queue._Queue)

        assert queue._ctx.has_direct_done_callback(queue._stop_waiter)

        assert isinstance(queue._collection, deque)

        assert not queue._waiter.is_set()

    async def test_can_stop_the_waiter_on_done(self, ctx: hp.CTX) -> None:
        queue = hp.queue(ctx=ctx)
        assert isinstance(queue, _queue._Queue)

        assert not queue._waiter.is_set()

        ctx.cancel()
        await asyncio.sleep(0.001)

        assert queue._waiter.is_set()

        # And if the waiter was already done
        queue = hp.queue(ctx=ctx)
        assert isinstance(queue, _queue._Queue)

        queue._waiter.set()

        ctx.cancel()
        await asyncio.sleep(0.001)

        assert queue._waiter.is_set()

    async def test_can_get_remaining_items(self, ctx: hp.CTX) -> None:
        queue = hp.queue(ctx=ctx)
        assert isinstance(queue, _queue._Queue)
        assert not queue._waiter.is_set()

        queue.append(1)
        assert queue._waiter.is_set()

        queue.append(2)

        assert list(queue.remaining()) == [1, 2]

        assert queue.is_empty()

    class TestGettingAllResults:
        async def test_can_get_results_until_ctx_is_done(
            self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
        ) -> None:
            wait = loop.create_future()

            queue = hp.queue(ctx=ctx)

            found = []

            async def fill() -> None:
                for i in (2, 3, 4):
                    queue.append(i)
                await wait
                for i in (5, 6, 7):
                    queue.append(i)

            with ctx.child(name="ff") as ff:
                async with hp.TaskHolder(ctx=ff) as ts:
                    ts.add(fill())

                    queue.append(1)

                    async for item in queue:
                        if item == 5:
                            queue.breaker.set()

                        found.append(item)

                        if item == 4:
                            wait.set_result(True)

            # The queue will drop remaining items
            assert found == [1, 2, 3, 4, 5]
            assert list(queue.remaining()) == [6, 7]

        async def test_ignores_results_added_after_ctx_is_done_if_still_waiting_for_results(
            self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
        ) -> None:
            wait = loop.create_future()

            queue = hp.queue(ctx=ctx)

            found = []

            async def fill() -> None:
                for i in (2, 3, 4):
                    queue.append(i)
                await wait
                queue.breaker.set()
                for i in (5, 6, 7):
                    queue.append(i)

            with ctx.child(name="ff") as ff:
                async with hp.TaskHolder(ctx=ff) as ts:
                    ts.add(fill())

                    queue.append(1)

                    async for item in queue:
                        found.append(item)

                        if item == 4:
                            wait.set_result(True)

            # The queue will drop remaining items
            assert found == [1, 2, 3, 4]
            assert list(queue.remaining()) == [5, 6, 7]

        async def test_is_re_entrant_if_we_break(self, ctx: hp.CTX) -> None:
            found = []
            queue = hp.queue(ctx=ctx)

            for i in range(10):
                queue.append(i)

            async for item in queue:
                found.append(item)

                if item == 3:
                    break

            assert found == [0, 1, 2, 3]

            async for item in queue:
                found.append(item)
                if item == 9:
                    queue.breaker.set()

            assert found == list(range(10))

    class TestGettingAllResultsAndEmptyOnFinished:
        async def test_can_get_results_until_ctx_is_done(
            self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
        ) -> None:
            wait = loop.create_future()

            queue = hp.queue(ctx=ctx, empty_on_finished=True)

            found = []

            async def fill() -> None:
                for i in (2, 3, 4):
                    queue.append(i)
                await wait
                for i in (5, 6, 7):
                    queue.append(i)

            with ctx.child(name="ff") as ff:
                async with hp.TaskHolder(ctx=ff) as ts:
                    ts.add(fill())

                    queue.append(1)

                    async for item in queue:
                        if item == 5:
                            queue.breaker.set()

                        found.append(item)

                        if item == 4:
                            wait.set_result(True)

            # The queue will not drop remaining items
            assert found == [1, 2, 3, 4, 5, 6, 7]
            assert list(queue.remaining()) == []

        async def test_gets_results_added_after_ctx_is_done_if_still_waiting_for_results(
            self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
        ) -> None:
            wait = loop.create_future()

            queue = hp.queue(ctx=ctx, empty_on_finished=True)

            found = []

            async def fill() -> None:
                for i in (2, 3, 4):
                    queue.append(i)
                await wait
                queue.breaker.set()
                for i in (5, 6, 7):
                    queue.append(i)

            with ctx.child(name="ff") as ff:
                async with hp.TaskHolder(ctx=ff) as ts:
                    ts.add(fill())

                    queue.append(1)

                    async for item in queue:
                        found.append(item)

                        if item == 4:
                            wait.set_result(True)

            # The queue will not drop remaining items
            assert found == [1, 2, 3, 4, 5, 6, 7]
            assert list(queue.remaining()) == []

        async def test_is_re_entrant_if_we_break(self, ctx: hp.CTX) -> None:
            found = []
            queue = hp.queue(ctx=ctx, empty_on_finished=True)

            for i in range(10):
                queue.append(i)

            async for item in queue:
                found.append(item)

                if item == 3:
                    break

            assert found == [0, 1, 2, 3]

            async for item in queue:
                found.append(item)
                if item == 9:
                    queue.breaker.set()

            assert found == list(range(10))

        async def test_can_append_with_priority(self, ctx: hp.CTX) -> None:
            found: list[object] = []
            queue = hp.queue(ctx=ctx)

            for i in range(10):
                queue.append(i)

            async for item in queue:
                found.append(item)
                if item == 3:
                    queue.append(50, priority=True)

                if item == 7:
                    queue.breaker.set()

            assert found == [0, 1, 2, 3, 50, 4, 5, 6, 7]

        async def test_can_be_given_functions_to_do_something_after_values_after_yielded(
            self, ctx: hp.CTX
        ) -> None:
            found: list[object] = []
            queue = hp.queue(ctx=ctx)

            for i in range(10):
                queue.append(i)

            def sneaky_stop(queue: hp.protocols.LimitedQueue) -> None:
                if len(queue) == 3:
                    queue.append(40)
                    queue.breaker.set()
                    found.append("sneaky_stop")

            queue.process_after_yielded(sneaky_stop)

            async for item in queue:
                found.append(item)
                found.append("-")

            assert list(queue.remaining()) == [7, 8, 9, 40]
            assert found == [0, "-", 1, "-", 2, "-", 3, "-", 4, "-", 5, "-", 6, "-", "sneaky_stop"]


class TestSyncQueue:
    def test_takes_in_a_ctx(self, ctx: hp.CTX) -> None:
        queue = hp.sync_queue(ctx=ctx)
        assert isinstance(queue, _queue._SyncQueue)

        assert queue._timeout == 0.05

        assert isinstance(queue._collection, NormalQueue)

        queue2 = hp.sync_queue(ctx=ctx, timeout=1)
        assert isinstance(queue2, _queue._SyncQueue)
        assert queue2._timeout == 1

    def test_can_append_items(self, ctx: hp.CTX) -> None:
        queue = hp.sync_queue(ctx=ctx)
        assert isinstance(queue, _queue._SyncQueue)

        queue.append(1)
        queue.append(2)

        found = []
        for item in queue:
            found.append(item)
            if item == 2:
                break

        assert found == [1, 2]

        queue.append(3)
        found = []
        for item in queue:
            found.append(item)
            ctx.cancel()
        assert found == [3]

    async def test_can_get_remaining_items(self, ctx: hp.CTX) -> None:
        queue = hp.sync_queue(ctx=ctx)

        queue.append(1)
        queue.append(2)

        assert list(queue.remaining()) == [1, 2]
        assert queue.is_empty()

    class TestGettingAllResults:
        async def test_can_get_results_until_ctx_is_done(
            self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
        ) -> None:
            wait = loop.create_future()

            queue = hp.sync_queue(ctx=ctx)

            found = []

            async def fill() -> None:
                for i in (2, 3, 4):
                    queue.append(i)
                await wait
                for i in (5, 6, 7):
                    queue.append(i)

            with ctx.child(name="ff") as ff:
                async with hp.TaskHolder(ctx=ff) as ts:
                    ts.add(fill())

                    queue.append(1)

                    for item in queue:
                        if item == 5:
                            ctx.cancel()

                        found.append(item)

                        if item == 4:
                            wait.set_result(True)

                        await asyncio.sleep(0.01)

            # The queue will drop remaining items
            assert found == [1, 2, 3, 4, 5]
            assert list(queue.remaining()) == [6, 7]

        async def test_ignores_results_added_after_ctx_is_done_if_still_waiting_for_results(
            self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
        ) -> None:
            wait = loop.create_future()

            queue = hp.sync_queue(ctx=ctx)

            found = []

            async def fill() -> None:
                for i in (2, 3, 4):
                    queue.append(i)
                await wait
                ctx.cancel()
                for i in (5, 6, 7):
                    queue.append(i)

            with ctx.child(name="ff") as ff:
                async with hp.TaskHolder(ctx=ff) as ts:
                    ts.add(fill())

                    queue.append(1)

                    for item in queue:
                        found.append(item)

                        if item == 4:
                            wait.set_result(True)

                        await asyncio.sleep(0.01)

            # The queue will drop remaining items
            assert found == [1, 2, 3, 4]
            assert list(queue.remaining()) == [5, 6, 7]

        async def test_is_re_entrant_if_we_break(self, ctx: hp.CTX) -> None:
            found = []
            queue = hp.sync_queue(ctx=ctx)

            for i in range(10):
                queue.append(i)

            for item in queue:
                found.append(item)

                if item == 3:
                    break

            assert found == [0, 1, 2, 3]

            for item in queue:
                found.append(item)
                if item == 9:
                    ctx.cancel()

            assert found == list(range(10))

    class TestGettingAllResultsWhenEmptyOnFinished:
        async def test_can_get_results_until_ctx_is_done(
            self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
        ) -> None:
            wait = loop.create_future()

            queue = hp.sync_queue(ctx=ctx, empty_on_finished=True)

            found = []

            async def fill() -> None:
                for i in (2, 3, 4):
                    queue.append(i)
                await wait
                for i in (5, 6, 7):
                    queue.append(i)

            with ctx.child(name="ff") as ff:
                async with hp.TaskHolder(ctx=ff) as ts:
                    ts.add(fill())

                    queue.append(1)

                    for item in queue:
                        if item == 5:
                            ctx.cancel()

                        found.append(item)

                        if item == 4:
                            wait.set_result(True)

                        await asyncio.sleep(0.01)

            # The queue will not drop remaining items
            assert found == [1, 2, 3, 4, 5, 6, 7]
            assert list(queue.remaining()) == []

        async def test_gets_results_added_after_ctx_is_done_if_still_waiting_for_results(
            self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
        ) -> None:
            wait = loop.create_future()

            queue = hp.sync_queue(ctx=ctx, empty_on_finished=True)

            found = []

            async def fill() -> None:
                for i in (2, 3, 4):
                    queue.append(i)
                await wait
                ctx.cancel()
                for i in (5, 6, 7):
                    queue.append(i)

            with ctx.child(name="ff") as ff:
                async with hp.TaskHolder(ctx=ff) as ts:
                    ts.add(fill())

                    queue.append(1)

                    for item in queue:
                        found.append(item)

                        if item == 4:
                            wait.set_result(True)

                        await asyncio.sleep(0.01)

            # The queue will not remaining items
            assert found == [1, 2, 3, 4, 5, 6, 7]
            assert list(queue.remaining()) == []

        async def test_is_re_entrant_if_we_break(self, ctx: hp.CTX) -> None:
            found = []
            queue = hp.sync_queue(ctx=ctx, empty_on_finished=True)

            for i in range(10):
                queue.append(i)

            for item in queue:
                found.append(item)

                if item == 3:
                    break

            assert found == [0, 1, 2, 3]

            for item in queue:
                found.append(item)
                if item == 9:
                    ctx.cancel()

            assert found == list(range(10))
