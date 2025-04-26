import asyncio
import logging
from collections.abc import AsyncGenerator, Iterator

import pytest

from machinery import helpers as hp


@pytest.fixture
def ctx() -> Iterator[hp.CTX]:
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    tramp: hp.protocols.Tramp = hp.Tramp(log=log)
    with hp.CTX.beginning(name="::", tramp=tramp) as ctx:
        yield ctx


class TestQueueFeeder:
    async def test_it_can_feed_values(self, ctx: hp.CTX) -> None:
        got: list[object] = []

        async with hp.queue_manager(ctx=ctx, make_empty_context=lambda: None) as (
            streamer,
            feeder,
        ):
            feeder.add_value(1)
            feeder.add_value(2)

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value):
                        got.append(value)

                        if value == 1:
                            feeder.add_value(3)

                        elif value == 3:
                            feeder.add_value(4)
                            feeder.set_as_finished_if_out_of_sources()
                            feeder.add_value(5)

                        elif value == 4:
                            feeder.add_value(6)

                    case hp.QueueManagerStopped():
                        got.append("stopped")

                    case _:
                        raise AssertionError(result)

        assert got == [1, 2, 3, 4, 5, 6, "stopped"]

    async def test_it_can_adds_stopped_after_queue_is_empty_even_if_values_added_after_told_to_finished_when_empty(
        self, ctx: hp.CTX
    ) -> None:
        got: list[object] = []

        async with hp.queue_manager(ctx=ctx, make_empty_context=lambda: None) as (
            streamer,
            feeder,
        ):
            feeder.add_value(1)
            feeder.add_value(2)
            feeder.set_as_finished_if_out_of_sources()

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value):
                        got.append(value)

                        if value == 1:
                            feeder.add_value(3)

                    case hp.QueueManagerStopped():
                        got.append("stopped")

                    case _:
                        raise AssertionError(result)

        assert got == [1, 2, 3, "stopped"]

    async def test_it_processes_stopped_before_everything_in_queue_on_manager_stopping(
        self, ctx: hp.CTX
    ) -> None:
        got: list[object] = []

        async with hp.queue_manager(ctx=ctx, make_empty_context=lambda: None) as (
            streamer,
            feeder,
        ):
            feeder.add_value(1)
            feeder.add_value(2)
            feeder.add_value(3)

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value):
                        got.append(value)

                        if value == 2:
                            ctx.cancel()
                            feeder.add_value(4)

                    case hp.QueueManagerStopped():
                        got.append("stopped")

                    case _:
                        raise AssertionError(result)

        assert got == [1, 2, "stopped", 3, 4]

    async def test_it_can_match_values_on_context(self, ctx: hp.CTX) -> None:
        got: list[object] = []

        async with hp.queue_manager(ctx=ctx, make_empty_context=lambda: "") as (
            streamer,
            feeder,
        ):
            feeder.add_value(1, context="one")
            feeder.add_value(2, context="two")

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value, context="one"):
                        got.append(f"ONE: {value}")
                        feeder.add_value(3, context="two")

                    case hp.QueueManagerSuccess(value=value, context="two"):
                        got.append(f"TWO: {value}")
                        if value == 3:
                            feeder.set_as_finished_if_out_of_sources()

                    case hp.QueueManagerStopped():
                        got.append("stopped")

                    case _:
                        raise AssertionError(result)

        assert got == ["ONE: 1", "TWO: 2", "TWO: 3", "stopped"]

    async def test_it_can_feed_in_a_task(self, ctx: hp.CTX) -> None:
        got: list[object] = []

        ready = asyncio.Event()

        async with hp.queue_manager(ctx=ctx, make_empty_context=lambda: "") as (streamer, feeder):

            async def stuff() -> str:
                await ready.wait()
                return "hi"

            feeder.add_task(ctx.loop.create_task(stuff()), context="some_context")
            feeder.add_value("ready")
            feeder.set_as_finished_if_out_of_sources()

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value, context=context):
                        got.append((value, context))

                match result:
                    case hp.QueueManagerSuccess(value="ready"):
                        ready.set()
                    case hp.QueueManagerSuccess(context="some_context"):
                        pass
                    case hp.QueueManagerStopped():
                        got.append("stopped")
                    case _:
                        raise AssertionError(result)

        assert got == [("ready", ""), ("hi", "some_context"), "stopped"]

    async def test_it_can_feed_in_a_coroutine(self, ctx: hp.CTX) -> None:
        got: list[object] = []

        ready = asyncio.Event()

        async with hp.queue_manager(ctx=ctx, make_empty_context=lambda: "") as (streamer, feeder):

            async def stuff() -> str:
                await ready.wait()
                return "hi"

            feeder.add_coroutine(stuff(), context="some_context")
            feeder.add_value("ready")
            feeder.set_as_finished_if_out_of_sources()

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value, context=context):
                        got.append((value, context))

                match result:
                    case hp.QueueManagerSuccess(value="ready"):
                        ready.set()
                    case hp.QueueManagerSuccess(context="some_context"):
                        pass
                    case hp.QueueManagerStopped():
                        got.append("stopped")
                    case _:
                        raise AssertionError(result)

        assert got == [("ready", ""), ("hi", "some_context"), "stopped"]

    async def test_it_can_feed_in_a_synchronous_function(self, ctx: hp.CTX) -> None:
        got: list[object] = []

        async with hp.queue_manager(ctx=ctx, make_empty_context=lambda: "") as (streamer, feeder):

            def stuff() -> str:
                return "hi"

            def other() -> str:
                return "other"

            feeder.add_sync_function(stuff, context="some_context")
            feeder.add_value("some_value")
            feeder.set_as_finished_if_out_of_sources()

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value, context=context):
                        got.append((value, context))
                        if value == "hi":
                            feeder.add_sync_function(other, context="things")
                    case hp.QueueManagerStopped():
                        got.append("stopped")
                    case _:
                        raise AssertionError(result)

        assert got == [("hi", "some_context"), ("some_value", ""), ("other", "things"), "stopped"]

    async def test_it_can_feed_in_a_synchronous_iterator(self, ctx: hp.CTX) -> None:
        got: list[object] = []

        async with hp.queue_manager(ctx=ctx, make_empty_context=lambda: "") as (streamer, feeder):

            def generator() -> Iterator[str]:
                yield "one"
                yield "two"
                yield "three"

            feeder.add_sync_iterator(generator(), context="a_generator")
            feeder.add_value("some_value")
            feeder.add_sync_iterator([1, 2, "three", 4], context="a_list")
            feeder.set_as_finished_if_out_of_sources()

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value, context=context):
                        got.append((value, context))
                        if value == 2 and context == "a_list":
                            feeder.add_value("another_value")
                        elif value == "two" and context == "a_generator":
                            feeder.add_value("yo")
                    case hp.QueueManagerIterationStop(context="a_generator"):
                        got.append("gen_stopped")
                    case hp.QueueManagerIterationStop(context="a_list"):
                        got.append("list_stopped")
                    case hp.QueueManagerStopped():
                        got.append("stopped")
                    case _:
                        raise AssertionError(result)

        assert got == [
            ("some_value", ""),
            ("one", "a_generator"),
            (1, "a_list"),
            ("two", "a_generator"),
            (2, "a_list"),
            ("yo", ""),
            ("three", "a_generator"),
            ("gen_stopped"),
            ("another_value", ""),
            ("three", "a_list"),
            (4, "a_list"),
            ("list_stopped"),
            "stopped",
        ]

    async def test_it_can_feed_in_an_asynchronous_generator(self, ctx: hp.CTX) -> None:
        got: list[object] = []

        async with hp.queue_manager(ctx=ctx, make_empty_context=lambda: "") as (streamer, feeder):

            async def generator() -> AsyncGenerator[str]:
                yield "one"
                yield "two"
                yield "three"

            feeder.add_async_generator(generator(), context="a_generator")
            feeder.add_value("some_value")
            feeder.add_sync_iterator([1, 2, "three", 4], context="a_list")
            feeder.set_as_finished_if_out_of_sources()

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value, context=context):
                        got.append((value, context))
                        if value == 2 and context == "a_list":
                            feeder.add_value("another_value")
                        elif value == "two" and context == "a_generator":
                            feeder.add_value("yo")
                    case hp.QueueManagerIterationStop(context="a_generator"):
                        got.append("gen_stopped")
                    case hp.QueueManagerIterationStop(context="a_list"):
                        got.append("list_stopped")
                    case hp.QueueManagerStopped():
                        got.append("stopped")
                    case _:
                        raise AssertionError(result)

        assert got == [
            ("some_value", ""),
            (1, "a_list"),
            ("one", "a_generator"),
            (2, "a_list"),
            ("two", "a_generator"),
            ("another_value", ""),
            ("three", "a_list"),
            ("yo", ""),
            ("three", "a_generator"),
            ("gen_stopped"),
            (4, "a_list"),
            ("list_stopped"),
            "stopped",
        ]
