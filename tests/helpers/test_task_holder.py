import asyncio
import logging
from collections.abc import Iterator

import pytest

from machinery import helpers as hp
from machinery import test_helpers as thp
from machinery._helpers import _task_holder


@pytest.fixture
def ctx() -> Iterator[hp.CTX]:
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    tramp: hp.protocols.Tramp = hp.Tramp(log=log)
    with hp.CTX.beginning(name="::", tramp=tramp) as ctx:
        yield ctx


class TestTaskHolder:
    async def test_takes_in_a_ctx(self, ctx: hp.CTX) -> None:
        async with hp.task_holder(ctx=ctx) as ts:
            assert list(ts) == []

    async def test_can_take_in_tasks(self, ctx: hp.CTX) -> None:
        called = []

        async def wait(amount: float) -> None:
            try:
                await asyncio.sleep(amount)
            finally:
                called.append(amount)

        async with hp.task_holder(ctx=ctx) as ts:
            ts.add_coroutine(wait(0.05))
            ts.add_coroutine(wait(0.01))

        assert called == [0.01, 0.05]

    async def test_exits_if_we_finish_all_tasks_before_the_manager_is_left(
        self, ctx: hp.CTX
    ) -> None:
        called = []

        async def wait(amount: float) -> None:
            try:
                await asyncio.sleep(amount)
            finally:
                called.append(amount)

        async with hp.task_holder(ctx=ctx) as ts:
            await ts.add_coroutine(wait(0.05))
            await ts.add_coroutine(wait(0.01))
            assert called == [0.05, 0.01]

        assert called == [0.05, 0.01]

    async def test_can_wait_for_more_tasks_if_they_are_added_when_the_manager_has_left(
        self, ctx: hp.CTX
    ) -> None:
        called = []

        async def wait(ts: hp.protocols.TaskHolder, amount: float) -> None:
            if amount == 0.01:
                ts.add_coroutine(wait(ts, 0.06))
            try:
                await asyncio.sleep(amount)
            finally:
                called.append(amount)

        async with hp.task_holder(ctx=ctx) as ts:
            ts.add_coroutine(wait(ts, 0.05))
            ts.add_coroutine(wait(ts, 0.01))

        assert called == [0.01, 0.05, 0.06]

    async def test_does_not_fail_if_a_task_raises_an_exception(self, ctx: hp.CTX) -> None:
        called = []

        async def wait(ts: hp.protocols.TaskHolder, amount: float) -> None:
            if amount == 0.01:
                ts.add_coroutine(wait(ts, 0.06))
            try:
                if amount == 0.06:
                    raise TypeError("WAT")
                await asyncio.sleep(amount)
            finally:
                called.append(amount)

        async with hp.task_holder(ctx=ctx) as ts:
            ts.add_coroutine(wait(ts, 0.05))
            ts.add_coroutine(wait(ts, 0.01))

        assert called == [0.06, 0.01, 0.05]

    async def test_stops_waiting_tasks_if_ctx_is_stopped(self, ctx: hp.CTX) -> None:
        called = []

        async def wait(ts: hp.protocols.TaskHolder, amount: float) -> None:
            try:
                await asyncio.sleep(amount)
                if amount == 0.05:
                    ctx.cancel()
            except asyncio.CancelledError:
                called.append(("CANCELLED", amount))
            finally:
                called.append(("FINISHED", amount))

        async with hp.task_holder(ctx=ctx) as ts:
            ts.add_coroutine(wait(ts, 5))
            ts.add_coroutine(wait(ts, 0.05))

        assert called == [("FINISHED", 0.05), ("CANCELLED", 5), ("FINISHED", 5)]

    async def test_can_say_how_many_pending_tasks_it_has(self, ctx: hp.CTX) -> None:
        called = []

        async def doit() -> None:
            await asyncio.sleep(1)

        async with hp.task_holder(ctx=ctx) as ts:
            assert ts.pending == 0
            t = ts.add_coroutine(doit())
            assert ts.pending == 1

            def process(res: hp.protocols.FutureStatus[None]) -> None:
                called.append(ts.pending)

            t.add_done_callback(process)
            t.cancel()

        assert called == [0]

    async def test_cancels_tasks_if_it_gets_cancelled(
        self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop
    ) -> None:
        called: list[object] = []
        waiter = loop.create_future()

        async def a_task(name: str) -> None:
            called.append(f"{name}_start")
            try:
                await loop.create_future()
            except asyncio.CancelledError:
                called.append(f"{name}_cancelled")
            except Exception as error:
                called.append((f"{name}_error", error))
            else:
                called.append(f"{name}_end")

        async def doit() -> None:
            async with hp.task_holder(ctx=ctx) as t:
                t.add_coroutine(a_task("one"))
                t.add_coroutine(a_task("two"))
                waiter.set_result(True)
                await loop.create_future()

        t = None
        try:
            t = ctx.async_as_background(doit())
            await waiter
            t.cancel()
        finally:
            if t:
                t.cancel()

        with pytest.raises(asyncio.CancelledError):
            await t
        assert called == ["one_start", "two_start", "one_cancelled", "two_cancelled"]

    async def test_can_iterate_tasks(self, ctx: hp.CTX) -> None:
        async with hp.task_holder(ctx=ctx) as ts:

            async def hi() -> None:
                pass

            t1 = ts.add_coroutine(hi())
            assert list(ts) == [t1]

            t2 = ts.add_coroutine(hi())
            t3 = ts.add_coroutine(hi())
            assert list(ts) == [t1, t2, t3]

    async def test_can_say_if_the_holder_has_a_task(self, ctx: hp.CTX) -> None:
        async with hp.task_holder(ctx=ctx) as ts:

            async def hi() -> None:
                pass

            t1 = ctx.async_as_background(hi())
            t2 = ctx.async_as_background(hi())

            assert t1 not in ts
            ts.add_task(t1)
            assert t1 in ts
            assert t2 not in ts

            ts.add_task(t2)
            assert t1 in ts
            assert t2 in ts

    async def test_can_clean_up_tasks(self, ctx: hp.CTX, loop: asyncio.AbstractEventLoop) -> None:
        called = []
        wait = loop.create_future()

        async def one() -> None:
            called.append("ONE")
            try:
                await asyncio.sleep(200)
            except asyncio.CancelledError:
                called.append("CANC_ONE")
                raise
            finally:
                called.append("FIN_ONE")

        async def two() -> None:
            called.append("TWO")
            try:
                await wait
                called.append("DONE_TWO")
            finally:
                called.append("FIN_TWO")

        async with hp.task_holder(ctx=ctx) as ts:
            t1 = ts.add_coroutine(one())
            ts.add_coroutine(two())

            assert called == []
            await asyncio.sleep(0)
            assert called == ["ONE", "TWO"]

            t1.cancel()
            await asyncio.sleep(0)
            assert called == ["ONE", "TWO", "CANC_ONE", "FIN_ONE"]

            wait.set_result(True)
            await asyncio.sleep(0)
            assert called == ["ONE", "TWO", "CANC_ONE", "FIN_ONE", "DONE_TWO", "FIN_TWO"]

    async def test_doesnt_lose_tasks_from_race_condition(self, ctx: hp.CTX) -> None:
        async with thp.mocked_call_later(ctx=ctx):
            called = []
            made = {}

            class TaskHolderManualClean(_task_holder._TaskHolder):
                async def cleaner(self) -> None:
                    await ctx.loop.create_future()

            async with TaskHolderManualClean(_ctx=ctx) as ts:

                async def one() -> None:
                    called.append("ONE")
                    try:
                        await asyncio.sleep(10)
                    except asyncio.CancelledError:
                        called.append("CANC_ONE")
                        raise
                    finally:
                        called.append("FIN_ONE")

                async def two() -> None:
                    called.append("TWO")
                    try:
                        await asyncio.sleep(200)
                    except asyncio.CancelledError:
                        called.append("CANC_TWO")
                        # Don't re-raise the exception to trigger race condition
                    finally:
                        called.append("FIN_TWO")

                t1 = ts.add_coroutine(two())

                def add_one(res: hp.protocols.FutureStatus[None]) -> None:
                    called.append("ADD_ONE")
                    made["t2"] = ts.add_coroutine(one())

                t1.add_done_callback(add_one)

                assert called == []
                await asyncio.sleep(0)
                assert called == ["TWO"]

                assert list(ts) == [t1]
                await ts._perform_clean()
                assert list(ts) == [t1]

                t1.cancel()
                await asyncio.sleep(0)

                assert called == ["TWO", "CANC_TWO", "FIN_TWO"]
                assert list(ts) == [t1]

                # The task holder only knows about t1
                # And after the clean, we expect it to have made the t2
                assert "t2" not in made
                await ts._perform_clean()
                assert called == ["TWO", "CANC_TWO", "FIN_TWO", "ADD_ONE", "ONE"]
                assert list(ts) == [made["t2"]]

                made["t2"].cancel()
                await asyncio.sleep(0)
                assert list(ts) == [made["t2"]]
                await ts._perform_clean()
                assert list(ts) == []
                assert called == [
                    "TWO",
                    "CANC_TWO",
                    "FIN_TWO",
                    "ADD_ONE",
                    "ONE",
                    "CANC_ONE",
                    "FIN_ONE",
                ]
