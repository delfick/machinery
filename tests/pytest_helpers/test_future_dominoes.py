import asyncio
import logging
from collections.abc import AsyncGenerator, Iterator

import pytest

from machinery import helpers as hp
from machinery import test_helpers as thp


@pytest.fixture
def ctx() -> Iterator[hp.CTX]:
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    tramp: hp.protocols.Tramp = hp.Tramp(log=log)
    with hp.CTX.beginning(name="::", tramp=tramp) as ctx:
        yield ctx


@pytest.fixture
async def task_holder(ctx: hp.CTX) -> hp.TaskHolder:
    with ctx.child(name="task_holder") as ctx_taskholder:
        async with hp.TaskHolder(ctx=ctx_taskholder) as task_holder:
            yield task_holder


class TestFutureDominoes:
    async def test_it_works(self, task_holder: hp.TaskHolder) -> None:
        called: list[object] = []
        finished = asyncio.Event()

        async with thp.future_dominoes(expected=8) as futs:

            async def one() -> None:
                await futs[1]
                called.append("first")
                await futs[2]
                called.append("second")
                await futs[5]
                called.append("fifth")
                await futs[7]
                called.append("seventh")

            async def two() -> AsyncGenerator[tuple[str, int]]:
                await futs[3]
                called.append("third")

                start = 4
                while start <= 6:
                    await futs[start]
                    called.append(("gen", start))
                    yield ("genresult", start)
                    start += 2

            async def three() -> None:
                await futs[8]
                called.append("final")
                finished.set()

            task_holder.add(three())
            task_holder.add(one())

            async def run_two() -> None:
                async for r in two():
                    called.append(r)

            task_holder.add(run_two())
            futs.begin()
            await futs.finished.wait()
            await finished.wait()

            assert called == [
                "first",
                "second",
                "third",
                ("gen", 4),
                ("genresult", 4),
                "fifth",
                ("gen", 6),
                ("genresult", 6),
                "seventh",
                "final",
            ]

    async def test_it_complains_if_not_all_futures_are_retrieved(
        self, task_holder: hp.TaskHolder
    ) -> None:
        called: list[object] = []

        async def incomplete() -> None:
            async with thp.future_dominoes(expected=4) as futs:
                finished = asyncio.Event()

                async def one() -> None:
                    await futs[1]
                    called.append("first")
                    await futs[2]
                    called.append("second")
                    finished.set()

                task_holder.add(one())
                futs.begin()
                await finished.wait()

        with pytest.raises(AssertionError) as e:
            await incomplete()

        assert str(e.value) == "Not all the futures were accessed: {3, 4}"

        assert called == [
            "first",
            "second",
        ]

    async def test_it_complains_if_not_all_futures_are_awaited(
        self, task_holder: hp.TaskHolder
    ) -> None:
        called: list[object] = []

        async def incomplete() -> None:
            async with thp.future_dominoes(expected=4) as futs:
                finished = asyncio.Event()

                async def one() -> None:
                    await futs[1]
                    called.append("first")
                    await futs[2]
                    futs[3]
                    futs[4]
                    called.append("second")
                    finished.set()

                task_holder.add(one())
                futs.begin()
                await finished.wait()

        with pytest.raises(AssertionError) as e:
            await incomplete()

        assert str(e.value) == "Not all the futures were completed: {3, 4}"

        assert called == [
            "first",
            "second",
        ]
