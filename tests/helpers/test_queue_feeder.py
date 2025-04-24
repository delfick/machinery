import logging
from collections.abc import Iterator

import pytest

from machinery import helpers as hp


@pytest.fixture
def ctx() -> Iterator[hp.CTX]:
    log = logging.getLogger()
    log.level = logging.INFO
    tramp: hp.protocols.Tramp = hp.Tramp(log=log)
    with hp.CTX.beginning(name="::", tramp=tramp) as ctx:
        yield ctx


class TestQueueFeeder:
    async def test_it_can_feed_values(self, ctx: hp.CTX) -> None:
        got: list[object] = []
        queue_manager = hp.QueueManager(ctx=ctx, make_empty_context=lambda: None)

        async with hp.TaskHolder(ctx=ctx) as ts:
            streamer, feeder = queue_manager.create_feeder(task_holder=ts)

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

                    case hp.QueueManagerStopped():
                        got.append("stopped")

                    case _:
                        raise AssertionError(result)

        assert got == [1, 2, 3, 4, 5, "stopped"]

    async def test_it_can_adds_stopped_after_queue_is_empty_even_if_values_added_after_told_to_finished_when_empty(
        self, ctx: hp.CTX
    ) -> None:
        got: list[object] = []
        queue_manager = hp.QueueManager(ctx=ctx, make_empty_context=lambda: None)

        async with hp.TaskHolder(ctx=ctx) as ts:
            streamer, feeder = queue_manager.create_feeder(task_holder=ts)

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
        queue_manager = hp.QueueManager(ctx=ctx, make_empty_context=lambda: None)

        async with hp.TaskHolder(ctx=ctx) as ts:
            streamer, feeder = queue_manager.create_feeder(task_holder=ts)

            feeder.add_value(1)
            feeder.add_value(2)
            feeder.add_value(3)

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess(value=value):
                        got.append(value)

                        if value == 2:
                            queue_manager.ctx.cancel()
                            feeder.add_value(4)

                    case hp.QueueManagerStopped():
                        got.append("stopped")

                    case _:
                        raise AssertionError(result)

        assert got == [1, 2, "stopped", 3, 4]

    async def test_it_can_match_values_on_context(self, ctx: hp.CTX) -> None:
        got: list[object] = []
        queue_manager = hp.QueueManager(ctx=ctx, make_empty_context=lambda: "")

        async with hp.TaskHolder(ctx=ctx) as ts:
            streamer, feeder = queue_manager.create_feeder(task_holder=ts)

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
