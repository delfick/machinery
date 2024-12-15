import asyncio
import inspect
import time
from collections import defaultdict
from contextvars import Context
from unittest import mock

from . import helpers as hp


def get_event_loop():
    return asyncio.get_event_loop_policy().get_event_loop()


class FakeTime:
    def __init__(self, mock_sleep=False, mock_async_sleep=False):
        self.time = 0
        self.patches = []
        self.mock_sleep = mock_sleep
        self.mock_async_sleep = mock_async_sleep
        self.original_time = time.time
        self.original_async_sleep = asyncio.sleep

    def set(self, t):
        self.time = round(t, 3)

    def add(self, t):
        self.time = round(self.time + t, 3)

    def __enter__(self):
        return self.start()

    def start(self):
        self.patches.append(mock.patch("time.time", self))

        if self.mock_sleep:
            self.patches.append(mock.patch("time.sleep", self.sleep))
        if self.mock_async_sleep:
            self.patches.append(mock.patch("asyncio.sleep", self.async_sleep))

        for p in self.patches:
            p.start()

        return self

    def __exit__(self, exc_typ, exc, tb):
        self.finish(exc_typ, exc, tb)

    def finish(self, exc_typ=None, exc=None, tb=None):
        for p in self.patches:
            p.stop()

    def __call__(self):
        return round(self.time, 3)

    def sleep(self, amount):
        self.add(amount)

    async def async_sleep(self, amount):
        self.add(amount)
        await self.original_async_sleep(0.001)


class MockedCallLater(hp.AsyncCMMixin):
    def __init__(self, t, precision=0.1):
        self.t = t
        self.loop = get_event_loop()
        self.precision = precision

        self.task = None
        self.call_later_patch = None
        self.create_future_patch = None

        self.funcs = []
        self.called_times = []
        self.have_call_later = self.hp.ResettableFuture()

    async def start(self):
        self.task = self.hp.async_as_background(self._calls())
        self.original_call_later = self.loop.call_later
        self.call_later_patch = mock.patch.object(self.loop, "call_later", self._call_later)
        self.call_later_patch.start()
        return self

    async def finish(self, exc_typ=None, exc=None, tb=None):
        if self.call_later_patch:
            self.call_later_patch.stop()
        if self.task:
            await self.hp.cancel_futures_and_wait(self.task, name="MockedCallLater.exit")

    async def add(self, amount):
        await self._run(iterations=round(amount / 0.1))

    async def resume_after(self, amount):
        fut = self.hp.create_future()
        get_event_loop().call_later(amount, fut.set_result, True)
        await fut

    @property
    def hp(self):
        return __import__("machinery").helpers

    def _call_later(self, when, func, *args):
        fr = inspect.currentframe()
        while fr and "tornado/" not in fr.f_code.co_filename:
            fr = fr.f_back
        if fr:
            return self.original_call_later(when, func, *args)

        called_from = inspect.currentframe().f_back.f_code.co_filename
        if any(exc in called_from for exc in ("alt_pytest_asyncio/",)):
            return self.original_call_later(when, func, *args)

        self.have_call_later.reset()
        self.have_call_later.set_result(True)

        info = {"cancelled": False}

        def caller():
            if not info["cancelled"]:
                self.called_times.append(time.time())
                func(*args)

        caller.original = func

        class Handle:
            def cancel(s):
                info["cancelled"] = True

        self.funcs.append((round(time.time() + when, 3), caller))
        return Handle()

    async def _allow_real_loop(self, until=0):
        while True:
            ready = get_event_loop()._ready
            ready_len = len(ready)
            await asyncio.sleep(0)
            if ready_len <= until:
                return

    async def _calls(self):
        await self.have_call_later

        while True:
            await self._allow_real_loop()
            await self.have_call_later
            await self._run()
            if not self.funcs:
                self.have_call_later.reset()

    async def _run(self, iterations=0):
        for iteration in range(iterations + 1):
            now = time.time()
            executed = False
            remaining = []

            for k, f in self.funcs:
                if now < k:
                    remaining.append((k, f))
                else:
                    executed = True
                    f()
                    await self._allow_real_loop(until=1)

            self.funcs = remaining

            if iterations >= 1 and iteration > 0:
                self.t.add(self.precision)

        if not executed and iterations == 0:
            self.t.add(self.precision)

        return executed


class FutureDominoes(hp.AsyncCMMixin):
    """
    A helper to start a domino of futures.

    For example:

    .. code-block:: python

        async def run():
            async with FutureDominoes(expected=8) as futs:
                called = []

                async def one():
                    await futs[1]
                    called.append("first")
                    await futs[2]
                    called.append("second")
                    await futs[5]
                    called.append("fifth")
                    await futs[7]
                    called.append("seventh")

                async def two():
                    await futs[3]
                    called.append("third")

                    start = 4
                    while start <= 6:
                        await futs[start]
                        called.append(("gen", start))
                        yield ("genresult", start)
                        start += 2

                async def three():
                    await futs[8]
                    called.append("final")

                loop = get_event_loop()
                loop.create_task(three())
                loop.create_task(one())

                async def run_two():
                    async for r in two():
                        called.append(r)

                loop.create_task(run_two())

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
    """

    def __init__(self, *, before_next_domino=None, expected):
        self.futs = {}
        self.retrieved = {}

        self.upto = 1
        self.expected = int(expected)
        self.before_next_domino = before_next_domino
        self.finished = self.hp.ResettableFuture()

        for i in range(self.expected):
            self.make(i + 1)

    async def start(self):
        self._tick = self.hp.async_as_background(self.tick())
        self._tick.add_done_callback(self.hp.transfer_result(self.finished))
        return self

    async def finish(self, exc_typ=None, exc=None, tb=None):
        if hasattr(self, "_tick"):
            if exc and not self._tick.done():
                self._tick.cancel()
            await self.hp.wait_for_all_futures(self._tick)

        if not exc:
            await self._tick

    async def tick(self):
        async with self.hp.tick(0, min_wait=0) as ticks:
            async for i, _ in ticks:
                await self.hp.wait_for_all_futures(self.retrieved[i], self.futs[i])
                print(f"Waited for Domino {i}")

                self.upto = i

                await self._allow_real_loop()

                if i >= self.expected:
                    print("Finished knocking over dominoes")
                    if not self.finished.done():
                        self.finished.set_result(True)

                if self.finished.done():
                    return

                self.make(i + 1)

                if self.before_next_domino:
                    self.before_next_domino(i)

                if not self.futs[i + 1].done():
                    self.futs[i + 1].set_result(True)

    async def _allow_real_loop(self):
        until = 0
        if "mock" in str(get_event_loop().call_later).lower():
            until = 1

        while True:
            ready = get_event_loop()._ready
            ready_len = len(ready)
            await asyncio.sleep(0)
            if ready_len <= until:
                return

    @property
    def hp(self):
        return __import__("machinery").helpers

    @property
    def loop(self):
        return get_event_loop()

    def make(self, num):
        if num > self.expected or self.finished.done():
            exc = Exception(f"Only expected up to {self.expected} dominoes")
            self.finished.reset()
            self.finished.set_exception(exc)
            raise exc

        if num in self.futs:
            return self.futs[num]

        fut = self.hp.create_future(name=f"Domino({num})")
        self.futs[num] = fut
        self.retrieved[num] = self.hp.create_future(name=f"Domino({num}.retrieved")
        fut.add_done_callback(self.hp.transfer_result(self.finished, errors_only=True))
        return fut

    def __getitem__(self, num):
        if not self.futs[1].done():
            self.futs[1].set_result(True)
        fut = self.make(num)
        if not self.retrieved[num].done():
            self.retrieved[num].set_result(True)
        return fut


def child_future_of(fut):
    hp = __import__("machinery").helpers

    class Compare:
        def __eq__(s, other):
            s.other = other
            s.eq = isinstance(other, hp.ChildOfFuture) and other.original_fut is fut
            return s.eq

        def __repr__(s):
            if not hasattr(s, "eq"):
                return f"<<COMPARE child of future {fut}>>"
            if not s.eq:
                return f"<<DIFFERENT got: {s.other.original_fut}, want: {fut}>>"
            return repr(s.other)

    return Compare()


def assertFutCallbacks(fut, *cbs, exhaustive=False):
    callbacks = fut._callbacks

    if not cbs:
        if callbacks:
            assert len(callbacks) == 1, f"Expect only one context callback: got {callbacks}"
            assert isinstance(
                callbacks[0], Context
            ), f"Expected just a context callback: got {callbacks}"

        return

    if not callbacks:
        assert False, f"expected callbacks, got {callbacks}"

    counts = defaultdict(lambda: 0)
    expected = defaultdict(lambda: 0)

    for cb in callbacks:
        if type(cb) is tuple:
            if len(cb) == 2 and isinstance(cb[1], Context):
                cb = cb[0]
            else:
                assert False, f"Got a tuple instead of a callback, {cb} in {callbacks}"

        if not isinstance(cb, Context):
            counts[cb] += 1

    for cb in cbs:
        expected[cb] += 1

    for cb in cbs:
        msg = f"Expected {expected[cb]} instances of {cb}, got {counts[cb]} in {callbacks}"
        assert counts[cb] == expected[cb], msg

    if exhaustive and len(callbacks) != len(cbs):
        assert False, f"Expected exactly {len(cbs)} callbacks but have {len(callbacks)}"
