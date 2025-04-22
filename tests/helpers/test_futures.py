import asyncio
import dataclasses
import time
import types
import uuid
from collections.abc import Sequence
from unittest import mock

import pytest

from machinery import helpers as hp
from machinery import test_helpers as thp


class TestCreatingAFuture:
    def test_can_create_a_future_from_a_provided_loop(self):
        fut = mock.Mock(name="future")
        loop = mock.Mock(name="loop")
        loop.create_future.return_value = fut
        assert hp.create_future(loop=loop) is fut
        assert fut.name is None
        loop.create_future.assert_called_once_with()

    def test_can_create_a_future_from_current_loop(self):
        fut = hp.create_future()
        assert isinstance(fut, asyncio.Future)
        assert fut.name is None

    def test_can_give_a_name_to_the_future(self):
        fut = hp.create_future(name="hi")
        assert fut.name == "hi"


class TestFutHasCallback:
    async def test_says_no_if_fut_has_no_callbacks(self, loop: asyncio.AbstractEventLoop) -> None:
        def func(res: hp.protocols.FutureStatus[None]) -> None:
            return None

        fut: asyncio.Future[None] = loop.create_future()
        assert not hp.fut_has_callback(fut, func)

    async def test_says_no_if_it_has_other_callbacks(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        def func1(res: hp.protocols.FutureStatus[None]) -> None:
            return None

        def func2(res: hp.protocols.FutureStatus[None]) -> None:
            return None

        fut: asyncio.Future[None] = loop.create_future()
        fut.add_done_callback(func1)
        assert not hp.fut_has_callback(fut, func2)

    async def test_says_yes_if_we_have_the_callback(self, loop: asyncio.AbstractEventLoop):
        def func1(res: hp.protocols.FutureStatus[None]) -> None:
            return None

        fut: asyncio.Future[None] = loop.create_future()
        fut.add_done_callback(func1)
        assert hp.fut_has_callback(fut, func1)

        def func2(res: hp.protocols.FutureStatus[None]) -> None:
            return None

        assert not hp.fut_has_callback(fut, func2)
        fut.add_done_callback(func2)
        assert hp.fut_has_callback(fut, func2)


class TestAsyncWithTimeout:
    async def test_returns_the_result_of_waiting_on_the_coroutine(self):
        val = str(uuid.uuid1())

        async def func():
            return val

        res = await hp.async_with_timeout(func(), timeout=10)
        assert res == val

    async def test_cancels_the_coroutine_if_it_doesnt_respond(self):
        async def func():
            await asyncio.sleep(2)

        start = time.time()
        with pytest.raises(asyncio.CancelledError):
            await hp.async_with_timeout(func(), timeout=0.1)
        assert time.time() - start < 0.5

    async def test_cancels_the_coroutine_and_raises_timeout_error(self):
        error = ValueError("Blah")

        async def func():
            try:
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                assert False, "Expected it to just raise the error rather than cancelling first"

        start = time.time()
        with pytest.raises(ValueError, match="Blah"):
            await hp.async_with_timeout(func(), timeout=0.1, timeout_error=error)
        assert time.time() - start < 0.5


class TestAsyncAsBackground:
    async def test_runs_the_coroutine_in_the_background(self):
        async def func(one, two, three=None):
            return f"{one}.{two}.{three}"

        t = hp.async_as_background(func(6, 5, three=9))
        thp.assertFutCallbacks(t, hp.reporter)
        assert isinstance(t, asyncio.Task)
        assert await t == "6.5.9"

    async def test_uses_silent_reporter_if_silent_is_True(self):
        async def func(one, two, three=None):
            return f"{one}.{two}.{three}"

        t = hp.async_as_background(func(6, 5, three=9), silent=True)
        thp.assertFutCallbacks(t, hp.silent_reporter)
        assert isinstance(t, asyncio.Task)
        assert await t == "6.5.9"


class TestSilentReporter:
    async def test_does_nothing_if_the_future_was_cancelled(self):
        fut = hp.create_future()
        fut.cancel()
        assert hp.silent_reporter(fut) is None

    async def test_does_nothing_if_the_future_has_an_exception(self):
        fut = hp.create_future()
        fut.set_exception(Exception("wat"))
        assert hp.silent_reporter(fut) is None

    async def test_returns_true_if_we_have_a_result(self):
        fut = hp.create_future()
        fut.set_result(mock.Mock(name="result"))
        assert hp.silent_reporter(fut) is True


class TestReporter:
    async def test_does_nothing_if_the_future_was_cancelled(self):
        fut = hp.create_future()
        fut.cancel()
        assert hp.reporter(fut) is None

    async def test_does_nothing_if_the_future_has_an_exception(self):
        fut = hp.create_future()
        fut.set_exception(Exception("wat"))
        assert hp.reporter(fut) is None

    async def test_returns_true_if_we_have_a_result(self):
        fut = hp.create_future()
        fut.set_result(mock.Mock(name="result"))
        assert hp.reporter(fut) is True


class TestTransferResult:
    async def test_works_as_a_done_callback(self, loop: asyncio.AbstractEventLoop) -> None:
        fut: asyncio.Future[list[int]] = loop.create_future()

        async def doit() -> list[int]:
            return [1, 2]

        t = loop.create_task(doit())
        t.add_done_callback(hp.transfer_result(fut))
        await t

        assert fut.result() == [1, 2]

    async def test_can_run_a_process_function(self, loop: asyncio.AbstractEventLoop) -> None:
        class Res:
            pass

        fut: asyncio.Future[Res] = loop.create_future()

        res = Res()

        async def doit() -> Res:
            return res

        def process(r: hp.protocols.FutureStatus[Res], f: asyncio.Future[Res]) -> None:
            assert r.result() is res
            assert f is fut
            assert f.result() is res

        t = loop.create_task(doit())
        t.add_done_callback(hp.transfer_result(fut, process=process))
        await t

        assert fut.result() is res

    class TestErrorsOnly:
        async def test_cancels_fut_if_res_is_cancelled(
            self, loop: asyncio.AbstractEventLoop
        ) -> None:
            fut = hp.create_future()
            res = hp.create_future()
            res.cancel()

            hp.transfer_result(fut, errors_only=True)(res)
            assert res.cancelled()

        async def test_sets_exception_on_fut_if_res_has_an_exception(
            self, loop: asyncio.AbstractEventLoop
        ) -> None:
            fut: asyncio.Future[None] = loop.create_future()
            res: asyncio.Future[None] = loop.create_future()

            error = ValueError("NOPE")
            res.set_exception(error)

            hp.transfer_result(fut, errors_only=True)(res)
            assert fut.exception() == error

        async def test_does_not_transfer_result(self, loop: asyncio.AbstractEventLoop) -> None:
            fut: asyncio.Future[list[int]] = loop.create_future()
            res: asyncio.Future[list[int]] = loop.create_future()
            res.set_result([1, 2])

            hp.transfer_result(fut, errors_only=True)(res)
            assert not fut.done()

    class TestNotErrorsOnly:
        async def test_cancels_fut_if_res_is_cancelled(
            self, loop: asyncio.AbstractEventLoop
        ) -> None:
            fut = hp.create_future()
            res = hp.create_future()
            res.cancel()

            hp.transfer_result(fut, errors_only=False)(res)
            assert res.cancelled()

        async def test_sets_exception_on_fut_if_res_has_an_exception(
            self, loop: asyncio.AbstractEventLoop
        ) -> None:
            fut: asyncio.Future[None] = loop.create_future()
            res: asyncio.Future[None] = loop.create_future()

            error = ValueError("NOPE")
            res.set_exception(error)

            hp.transfer_result(fut, errors_only=False)(res)
            assert fut.exception() == error

        async def test_transfers_result(self, loop: asyncio.AbstractEventLoop) -> None:
            fut: asyncio.Future[list[int]] = loop.create_future()
            res: asyncio.Future[list[int]] = loop.create_future()
            res.set_result([1, 2])

            hp.transfer_result(fut, errors_only=False)(res)
            assert fut.result() == [1, 2]


class TestNoncancelledResultsFromFuts:
    async def test_returns_results_from_done_futures_that_arent_cancelled(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        @dataclasses.dataclass(frozen=True, kw_only=True)
        class Res:
            name: str

        fut1: asyncio.Future[Res] = loop.create_future()
        fut2: asyncio.Future[Res] = loop.create_future()
        fut3: asyncio.Future[Res] = loop.create_future()
        fut4: asyncio.Future[Res] = loop.create_future()

        result1 = Res(name="result1")
        result2 = Res(name="result2")

        fut2.set_result(result1)
        fut3.cancel()
        fut4.set_result(result2)

        assert hp.noncancelled_results_from_futs([fut1, fut2, fut3, fut4]) == (
            None,
            [result1, result2],
        )

    async def test_returns_found_errors_as_well(self, loop: asyncio.AbstractEventLoop) -> None:
        @dataclasses.dataclass(frozen=True, kw_only=True)
        class Res:
            name: str

        fut1: asyncio.Future[Res] = loop.create_future()
        fut2: asyncio.Future[Res] = loop.create_future()
        fut3: asyncio.Future[Res] = loop.create_future()
        fut4: asyncio.Future[Res] = loop.create_future()

        error1 = Exception("wat")
        result2 = Res(name="result2")

        fut2.set_exception(error1)
        fut3.cancel()
        fut4.set_result(result2)

        assert hp.noncancelled_results_from_futs([fut1, fut2, fut3, fut4]) == (error1, [result2])

    async def test_squashes_the_same_error_into_one_error(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        fut1: asyncio.Future[None] = loop.create_future()
        fut2: asyncio.Future[None] = loop.create_future()
        fut3: asyncio.Future[None] = loop.create_future()
        fut4: asyncio.Future[None] = loop.create_future()

        error1 = ValueError("wat one=1")

        class OtherError(Exception):
            def __eq__(self, o: object) -> bool:
                return o is error1

        error2 = OtherError()

        fut2.set_exception(error1)
        fut3.cancel()
        fut4.set_exception(error2)

        assert hp.noncancelled_results_from_futs([fut1, fut2, fut3, fut4]) == (error1, [])

    async def test_can_return_error_with_multiple_errors(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        @dataclasses.dataclass(frozen=True, kw_only=True)
        class Res:
            name: str

        fut1: asyncio.Future[Res] = loop.create_future()
        fut2: asyncio.Future[Res] = loop.create_future()
        fut3: asyncio.Future[Res] = loop.create_future()
        fut4: asyncio.Future[Res] = loop.create_future()
        fut5: asyncio.Future[Res] = loop.create_future()

        error1 = ValueError("wat")
        error2 = ValueError("wat2")
        result2 = Res(name="result2")

        fut2.set_exception(error1)
        fut3.cancel()
        fut4.set_result(result2)
        fut5.set_exception(error2)

        result = hp.noncancelled_results_from_futs([fut1, fut2, fut3, fut4, fut5])
        assert isinstance(result[0], BaseExceptionGroup)
        assert result[0].exceptions == (error1, error2)
        assert result[1] == [result2]


class TestFindAndApplyResult:
    @dataclasses.dataclass(frozen=True, kw_only=True)
    class Res:
        name: str

    class PerTestLogic[T_Res = Res]:
        def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
            self.fut1: asyncio.Future[T_Res] = loop.create_future()
            self.fut2: asyncio.Future[T_Res] = loop.create_future()
            self.fut3: asyncio.Future[T_Res] = loop.create_future()
            self.fut4: asyncio.Future[T_Res] = loop.create_future()
            self.final_fut: asyncio.Future[T_Res] = loop.create_future()
            self.Res = TestFindAndApplyResult.Res

        @property
        def available_futs(self) -> Sequence[asyncio.Future[T_Res]]:
            return [self.fut1, self.fut2, self.fut3, self.fut4]

    async def test_cancels_futures_if_final_future_is_cancelled(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        test_logic = self.PerTestLogic(loop)

        test_logic.final_fut.cancel()
        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is False

        assert test_logic.fut1.cancelled()
        assert test_logic.fut2.cancelled()
        assert test_logic.fut3.cancelled()
        assert test_logic.fut4.cancelled()

        assert test_logic.final_fut.cancelled()

    async def test_sets_exceptions_on_futures_if_final_future_has_an_exception(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        test_logic = self.PerTestLogic(loop)

        error = ValueError("NOPE")
        test_logic.final_fut.set_exception(error)
        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is False

        for f in test_logic.available_futs:
            assert f.exception() is error

    async def test_ignores_futures_already_done_when_final_future_has_an_exception(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        test_logic = self.PerTestLogic[list[int]](loop)

        err1 = Exception("LOLZ")
        test_logic.available_futs[0].set_exception(err1)
        test_logic.available_futs[1].cancel()
        test_logic.available_futs[2].set_result([1, 2])

        err2 = ValueError("NOPE")
        test_logic.final_fut.set_exception(err2)
        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is False

        assert test_logic.available_futs[0].exception() is err1
        assert test_logic.available_futs[1].cancelled()
        assert test_logic.available_futs[2].result() == [1, 2]
        assert test_logic.available_futs[3].exception() is err2

    async def test_spreads_error_if_any_is_found(self, loop: asyncio.AbstractEventLoop) -> None:
        test_logic = self.PerTestLogic(loop)

        error1 = Exception("wat")
        test_logic.fut2.set_exception(error1)

        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is True

        assert test_logic.fut1.exception() is error1
        assert test_logic.fut2.exception() is error1
        assert test_logic.fut3.exception() is error1
        assert test_logic.fut4.exception() is error1

        assert test_logic.final_fut.exception() is error1

    async def test_doesnt_spread_error_to_those_already_cancelled_or_with_error(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        test_logic = self.PerTestLogic(loop)

        error1 = ValueError("wat")
        test_logic.fut2.set_exception(error1)

        error2 = ValueError("wat2")
        test_logic.fut1.set_exception(error2)

        test_logic.fut4.cancel()

        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is True

        assert test_logic.fut1.exception() is error2
        assert test_logic.fut2.exception() is error1

        exception_3 = test_logic.fut3.exception()
        assert isinstance(exception_3, ExceptionGroup)
        assert exception_3.exceptions == (error2, error1)

        assert test_logic.fut4.cancelled()

        exception_final = test_logic.final_fut.exception()
        assert isinstance(exception_final, ExceptionGroup)
        assert exception_final.exceptions == (error2, error1)

    async def test_sets_results_if_one_has_a_result(self, loop: asyncio.AbstractEventLoop) -> None:
        test_logic = self.PerTestLogic(loop)

        result = test_logic.Res(name="result")
        test_logic.fut1.set_result(result)

        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is True

        assert test_logic.fut1.result() is result
        assert test_logic.fut2.result() is result
        assert test_logic.fut3.result() is result
        assert test_logic.fut4.result() is result

        assert test_logic.final_fut.result() is result

    async def test_sets_results_if_one_has_a_result_except_for_cancelled_ones(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        test_logic = self.PerTestLogic(loop)

        result = test_logic.Res(name="result")
        test_logic.fut1.set_result(result)
        test_logic.fut2.cancel()

        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is True

        assert test_logic.fut1.result() is result
        assert test_logic.fut2.cancelled()
        assert test_logic.fut3.result() is result
        assert test_logic.fut4.result() is result

        assert test_logic.final_fut.result() is result

    async def test_sets_result_on_final_fut_unless_its_already_cancelled(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        test_logic = self.PerTestLogic(loop)

        result = test_logic.Res(name="result")
        test_logic.fut1.set_result(result)
        test_logic.final_fut.cancel()

        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is False
        assert test_logic.final_fut.cancelled()

    async def test_cancels_final_fut_if_any_of_our_futs_are_cancelled(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        test_logic = self.PerTestLogic(loop)

        test_logic.fut1.cancel()
        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is True
        assert test_logic.final_fut.cancelled()

    async def test_does_nothing_if_none_of_the_futures_are_done(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        test_logic = self.PerTestLogic(loop)

        assert hp.find_and_apply_result(test_logic.final_fut, test_logic.available_futs) is False
        for f in test_logic.available_futs:
            assert not f.done()
        assert not test_logic.final_fut.done()


class TestWaitingForAllFutures:
    async def test_does_nothing_if_there_are_no_futures(self):
        await hp.wait_for_all_futures()

    async def test_waits_for_all_the_futures_to_be_complete_regardless_of_status(self):
        """Deliberately don't wait on futures to ensure we don't get warnings if they aren't awaited"""
        fut1 = hp.create_future()
        fut2 = hp.create_future()
        fut3 = hp.create_future()
        fut4 = hp.create_future()

        w = hp.async_as_background(hp.wait_for_all_futures(fut1, fut2, fut3, fut3, fut4))
        await asyncio.sleep(0.01)
        assert not w.done()

        fut1.set_result(True)
        await asyncio.sleep(0.01)
        assert not w.done()

        fut2.set_exception(Exception("yo"))
        await asyncio.sleep(0.01)
        assert not w.done()

        fut3.cancel()
        await asyncio.sleep(0.01)
        assert not w.done()

        fut4.set_result(False)

        await asyncio.sleep(0.01)
        assert w.done()
        await w

        assert not any(f._callbacks for f in (fut1, fut2, fut3, fut4))


class TestWaitingForFirstFuture:
    async def test_does_nothing_if_there_are_no_futures(self):
        await hp.wait_for_first_future()

    async def test_returns_if_any_of_the_futures_are_already_done(self):
        fut1 = hp.create_future()
        fut2 = hp.create_future()
        fut3 = hp.create_future()

        fut2.set_result(True)
        await hp.wait_for_first_future(fut1, fut2, fut3)
        assert not fut2._callbacks
        assert all(len(f._callbacks) == 1 for f in (fut1, fut3))

    async def test_returns_on_the_first_future_to_have_a_result(self):
        fut1 = hp.create_future()
        fut2 = hp.create_future()
        fut3 = hp.create_future()

        w = hp.async_as_background(hp.wait_for_first_future(fut1, fut2, fut3))
        await asyncio.sleep(0.01)
        assert not w.done()

        fut2.set_result(True)
        await fut2
        await asyncio.sleep(0.01)
        assert w.done()

        await w
        assert not fut2._callbacks
        assert all(len(f._callbacks) == 1 for f in (fut1, fut3))

    async def test_returns_on_the_first_future_to_have_an_exception(self):
        fut1 = hp.create_future()
        fut2 = hp.create_future()
        fut3 = hp.create_future()

        w = hp.async_as_background(hp.wait_for_first_future(fut1, fut2, fut3))
        await asyncio.sleep(0.01)
        assert not w.done()

        fut3.set_exception(ValueError("NOPE"))
        with pytest.raises(ValueError, match="NOPE"):
            await fut3
        await asyncio.sleep(0.01)
        assert w.done()

        await w
        assert not fut3._callbacks
        assert all(len(f._callbacks) == 1 for f in (fut1, fut2))

    async def test_returns_on_the_first_future_to_be_cancelled(self):
        fut1 = hp.create_future()
        fut2 = hp.create_future()
        fut3 = hp.create_future()

        w = hp.async_as_background(hp.wait_for_first_future(fut1, fut2, fut3))
        await asyncio.sleep(0.01)
        assert not w.done()

        fut1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await fut1
        await asyncio.sleep(0.01)
        assert w.done()

        await w
        assert not fut1._callbacks
        assert all(len(f._callbacks) == 1 for f in (fut2, fut3))


class TestEnsuringAexit:
    async def test_ensures_aexit_is_called_on_exception(self) -> None:
        error = Exception("NOPE")
        called: list[str] = []

        class Thing:
            async def __aenter__(s) -> None:
                called.append("aenter")
                await s.start()

            async def start(s) -> None:
                raise error

            async def __aexit__(
                s,
                exc_typ: type[BaseException] | None,
                exc: BaseException | None,
                tb: types.TracebackType,
            ) -> None:
                called.append("aexit")
                assert exc is error

        with pytest.raises(Exception, match="NOPE"):
            async with Thing():
                called.append("inside")

        assert called == ["aenter"]
        called.clear()

        # But with our special context manager

        error = Exception("NOPE")
        called = []

        class Thing2:
            async def __aenter__(s) -> None:
                called.append("aenter")
                async with hp.ensure_aexit(s):
                    await s.start()

            async def start(self) -> None:
                raise error

            async def __aexit__(
                s,
                exc_typ: type[BaseException] | None,
                exc: BaseException | None,
                tb: types.TracebackType | None,
            ) -> None:
                called.append("aexit")
                assert exc is error

        with pytest.raises(Exception, match="NOPE"):
            async with Thing2():
                called.append("inside")

        assert called == ["aenter", "aexit"]

    async def test_doesnt_call_exit_twice_on_success(self) -> None:
        called = []

        class Thing:
            async def __aenter__(s) -> None:
                called.append("aenter")
                async with hp.ensure_aexit(s):
                    await s.start()

            async def start(self) -> None:
                called.append("start")

            async def __aexit__(
                s,
                exc_typ: type[BaseException] | None,
                exc: BaseException | None,
                tb: types.TracebackType | None,
            ) -> None:
                called.append("aexit")
                assert exc is None

        async with Thing():
            called.append("inside")

        assert called == ["aenter", "start", "inside", "aexit"]
