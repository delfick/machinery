# coding: spec

import asyncio

import pytest

from machinery import helpers as hp

describe "ResettableFuture":
    async it "ensure_future returns the ResettableFuture as is":
        fut = hp.ResettableFuture()
        assert asyncio.ensure_future(fut) is fut

    async it "creates a future":
        fut = hp.ResettableFuture()
        assert isinstance(fut.fut, asyncio.Future)
        assert fut.name is None
        assert fut.fut.name == "ResettableFuture(None)::__init__[fut]"

        fut = hp.ResettableFuture(name="blah")
        assert fut.name == "blah"
        assert isinstance(fut.fut, asyncio.Future)
        assert fut.fut.name == "ResettableFuture(blah)::__init__[fut]"

    async it "gets callbacks from the current future", assertFutCallbacks:
        fut = hp.ResettableFuture()
        assert len(fut._callbacks) == 1
        assertFutCallbacks(fut, hp.silent_reporter)
        assert fut._callbacks == fut.fut._callbacks

    async it "knows if the future is done":
        fut = hp.ResettableFuture()
        assert not fut.done()
        fut.set_result(True)
        assert fut.done()
        fut.reset()
        assert not fut.done()

        fut.set_exception(TypeError("NOPE"))
        assert fut.done()
        fut.reset()
        assert not fut.done()

        fut.cancel()
        assert fut.done()
        fut.reset()
        assert not fut.done()

    async it "can get and set a result":
        fut = hp.ResettableFuture()
        fut.set_result(True)
        assert fut.result() is True

        with pytest.raises(hp.InvalidStateError):
            fut.set_result(False)

        assert await fut is True

        fut.reset()
        assert not fut.done()
        fut.set_result(False)
        assert fut.result() is False

        assert await fut is False

    async it "can get and set an exception":
        fut = hp.ResettableFuture()
        error = ValueError("NOPE")
        fut.set_exception(error)
        assert fut.exception() is error

        with pytest.raises(hp.InvalidStateError):
            fut.set_exception(TypeError("HI"))

        with pytest.raises(ValueError, match="NOPE"):
            await fut

        fut.reset()
        assert not fut.done()
        error2 = TypeError("HI")
        fut.set_exception(error2)
        assert fut.exception() is error2

        with pytest.raises(TypeError, match="HI"):
            await fut

    async it "can be cancelled and be asked if cancelled":
        fut = hp.ResettableFuture()
        fut.cancel()
        assert fut.cancelled()

        fut.cancel()
        assert fut.cancelled()

        with pytest.raises(asyncio.CancelledError):
            await fut

        fut.reset()
        assert not fut.done()

        fut.cancel()
        with pytest.raises(asyncio.CancelledError):
            await fut

    async it "can have done callbacks":
        fut = hp.ResettableFuture()

        called = []

        def one(res):
            called.append("ONE")

        def two(res):
            called.append("TWO")

        def three(res):
            called.append("THREE")

        fut.add_done_callback(one)
        fut.add_done_callback(two)
        fut.add_done_callback(three)

        fut.set_result(True)
        await asyncio.sleep(0)
        assert not fut._callbacks
        assert called == ["ONE", "TWO", "THREE"]

        fut.reset()
        called.clear()
        fut.add_done_callback(one)
        fut.add_done_callback(two)
        fut.add_done_callback(three)

        assert fut._callbacks and len(fut._callbacks) == 4
        fut.remove_done_callback(two)
        assert fut._callbacks and len(fut._callbacks) == 3

        fut.cancel()
        await asyncio.sleep(0)
        assert not fut._callbacks
        assert called == ["ONE", "THREE"]

        fut.reset()
        called.clear()
        fut.set_exception(TypeError("NOPE"))
        await asyncio.sleep(0)
        fut.add_done_callback(one)
        fut.add_done_callback(two)
        await asyncio.sleep(0)

        assert called == ["ONE", "TWO"]

    async it "has a repr":
        fut = hp.ResettableFuture()
        assert repr(fut) == "<ResettableFuture#None((pending))>"

        fut = hp.ResettableFuture(name="hello")
        assert repr(fut) == "<ResettableFuture#hello((pending))>"

    describe "reset":
        async it "does nothing if the future hasn't been resolved yet", assertFutCallbacks:
            fut = hp.ResettableFuture()
            f = fut.fut

            fut.reset()
            assert fut.fut is f

            called = []

            cb = lambda res: called.append("DONE1")
            fut.add_done_callback(cb)
            assertFutCallbacks(f, cb, hp.silent_reporter)
            assertFutCallbacks(fut, cb, hp.silent_reporter)

            fut.reset()
            assertFutCallbacks(f, cb, hp.silent_reporter)
            assertFutCallbacks(fut, cb, hp.silent_reporter)

            await asyncio.sleep(0)
            assert called == []
            assertFutCallbacks(f, cb, hp.silent_reporter)
            assertFutCallbacks(fut, cb, hp.silent_reporter)

            fut.set_result(True)
            await asyncio.sleep(0)
            assert called == ["DONE1"]
            assertFutCallbacks(f)
            assertFutCallbacks(fut)

        async it "can force the future to be closed", assertFutCallbacks:
            fut = hp.ResettableFuture()
            f = fut.fut

            called = []
            cb = lambda res: called.append("DONE1")
            fut.add_done_callback(cb)
            assertFutCallbacks(f, cb, hp.silent_reporter)
            assertFutCallbacks(fut, cb, hp.silent_reporter)

            fut.reset(force=True)
            await asyncio.sleep(0)
            assert fut.fut is not f
            assert called == ["DONE1"]
            assertFutCallbacks(fut.fut, hp.silent_reporter)
            assertFutCallbacks(fut, hp.silent_reporter)
