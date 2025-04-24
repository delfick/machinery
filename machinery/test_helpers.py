import asyncio
import contextvars
import inspect
import logging
import time
import types
from collections.abc import Callable
from typing import Protocol, Self, Unpack
from unittest import mock

from . import helpers as hp


class _CallLater(Protocol):
    def __call__[*T_Args](
        self,
        delay: float,
        callback: Callable[[Unpack[T_Args]], object],
        *args: *T_Args,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle: ...


class _CallableWithOriginal(Protocol):
    @property
    def original(self) -> Callable[..., object]: ...

    def __call__(self) -> None: ...


class Cancellable(Protocol):
    def cancel(self) -> None: ...


log = logging.getLogger()


class FakeTime:
    def __init__(self, mock_sleep: bool = False, mock_async_sleep: bool = False) -> None:
        self.time: float = 0
        self.patches: list[mock._patch[object]] = []
        self.mock_sleep = mock_sleep
        self.mock_async_sleep = mock_async_sleep
        self.original_time = time.time
        self.original_async_sleep = asyncio.sleep

    def set(self, t: float) -> None:
        self.time = round(t, 3)

    def add(self, t: float) -> None:
        self.time = round(self.time + t, 3)

    def __enter__(self) -> Self:
        return self.start()

    def start(self) -> Self:
        self.patches.append(mock.patch("time.time", self))

        if self.mock_sleep:
            self.patches.append(mock.patch("time.sleep", self.sleep))
        if self.mock_async_sleep:
            self.patches.append(mock.patch("asyncio.sleep", self.async_sleep))

        for p in self.patches:
            p.start()

        return self

    def __exit__(
        self,
        exc_typ: type[BaseException] | None,
        exc: BaseException | None,
        tb: types.TracebackType | None,
    ) -> None:
        self.finish(exc_typ, exc, tb)

    def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        for p in self.patches:
            p.stop()

    def __call__(self) -> float:
        return round(self.time, 3)

    def sleep(self, amount: float) -> None:
        self.add(amount)

    async def async_sleep(self, amount: float) -> None:
        self.add(amount)
        await self.original_async_sleep(0.001)


class MockedCallLater:
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        return await self.finish(exc_type, exc, tb)

    async def __aenter__(self) -> Self:
        async with hp.ensure_aexit(self):
            return await self.start()

    original_call_later: _CallLater

    def __init__(
        self, t: FakeTime, loop: asyncio.AbstractEventLoop, precision: float = 0.1
    ) -> None:
        self.t = t
        self.loop = loop
        self.precision = precision

        tramp = hp.Tramp(log=log)
        self.ctx = hp.CTX.beginning(name="::", tramp=tramp)

        self.task: asyncio.Task[None] | None = None
        self.call_later_patch: mock._patch[object] | None = None
        self.create_future_patch: mock._patch[object] | None = None

        self.funcs: list[tuple[float, _CallableWithOriginal]] = []
        self.called_times: list[float] = []
        self.have_call_later = asyncio.Event()

    async def start(self) -> Self:
        self.task = self.ctx.async_as_background(self._calls())
        self.original_call_later = self.loop.call_later
        self.call_later_patch = mock.patch.object(self.loop, "call_later", self._call_later)
        self.call_later_patch.start()
        return self

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        if self.call_later_patch:
            self.call_later_patch.stop()
        if self.task:
            self.task.cancel()
            await self.ctx.wait_for_all_futures(self.task)

    async def add(self, amount: float) -> None:
        await self._run(iterations=round(amount / 0.1))

    async def resume_after(self, amount: float) -> None:
        fut = self.loop.create_future()
        self.loop.call_later(amount, fut.set_result, True)
        await fut

    def _call_later[*T_Args, T_Ret](
        self, when: float, func: Callable[[Unpack[T_Args]], T_Ret], *args: *T_Args
    ) -> Cancellable:
        fr = inspect.currentframe()
        while fr and "tornado/" not in fr.f_code.co_filename:
            fr = fr.f_back
        if fr:
            return self.original_call_later(when, func, *args)

        current_frame = inspect.currentframe()
        assert current_frame is not None
        frame_back = current_frame.f_back
        assert frame_back is not None
        frame_code = frame_back.f_code
        assert frame_code is not None

        called_from = frame_code.co_filename
        if any(exc in called_from for exc in ("alt_pytest_asyncio/",)):
            return self.original_call_later(when, func, *args)

        self.have_call_later.set()

        info = {"cancelled": False}

        class Caller:
            def __init__(s) -> None:
                self.original = func

            def __call__(s) -> None:
                if not info["cancelled"]:
                    self.called_times.append(time.time())
                    func(*args)

        class Handle:
            def cancel(s) -> None:
                info["cancelled"] = True

        self.funcs.append((round(time.time() + when, 3), Caller()))
        return Handle()

    async def _allow_real_loop(self, until: float = 0) -> None:
        while True:
            ready = self.loop._ready  # type: ignore[attr-defined]
            ready_len = len(ready)
            await asyncio.sleep(0)
            if ready_len <= until:
                return

    async def _calls(self) -> None:
        await self.have_call_later.wait()

        while True:
            await self._allow_real_loop()
            await self.have_call_later.wait()
            await self._run()
            if not self.funcs:
                self.have_call_later.clear()

    async def _run(self, iterations: int = 0) -> bool:
        for iteration in range(iterations + 1):
            now = time.time()
            executed = False
            remaining: list[tuple[float, _CallableWithOriginal]] = []

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


class FutureDominoes:
    """
    A helper to start a domino of futures.

    For example:

    .. code-block:: python

        async def run():
            async with FutureDominoes(loop=loop, expected=8) as futs:
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

                loop = ...
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

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        return await self.finish(exc_type, exc, tb)

    async def __aenter__(self) -> Self:
        async with hp.ensure_aexit(self):
            return await self.start()

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        before_next_domino: Callable[[int], None] | None = None,
        expected: int,
    ) -> None:
        self.futs: dict[int, asyncio.Future[None]] = {}
        self.retrieved: dict[int, asyncio.Future[None]] = {}
        self.loop = loop

        self.upto = 1
        self.expected = expected
        self.before_next_domino = before_next_domino
        self.finished: asyncio.Future[None] = self.loop.create_future()

        tramp = hp.Tramp(log=log)
        self.ctx = hp.CTX.beginning(name="::", tramp=tramp)

        for i in range(self.expected):
            self.make(i + 1)

    async def start(self) -> Self:
        self._tick = self.ctx.async_as_background(self.tick())
        self._tick.add_done_callback(hp.transfer_result(self.finished))
        return self

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        if hasattr(self, "_tick"):
            if exc and not self._tick.done():
                self._tick.cancel()
            await self.ctx.wait_for_all_futures(self._tick)

        if not exc:
            await self._tick

    async def tick(self) -> None:
        async with hp.Ticker(0, ctx=self.ctx, min_wait=0) as ticks:
            async for i, _ in ticks:
                await self.ctx.wait_for_all_futures(self.retrieved[i], self.futs[i])
                print(f"Waited for Domino {i}")  # noqa: T201

                self.upto = i

                await self._allow_real_loop()

                if i >= self.expected:
                    print("Finished knocking over dominoes")  # noqa: T201
                    if not self.finished.done():
                        self.finished.set_result(None)

                if self.finished.done():
                    return

                self.make(i + 1)

                if self.before_next_domino:
                    self.before_next_domino(i)

                if not self.futs[i + 1].done():
                    self.futs[i + 1].set_result(None)

    async def _allow_real_loop(self) -> None:
        until = 0
        if "mock" in str(self.loop.call_later).lower():
            until = 1

        while True:
            ready = self.loop._ready  # type: ignore[attr-defined]
            ready_len = len(ready)
            await asyncio.sleep(0)
            if ready_len <= until:
                return

    def make(self, num: int) -> asyncio.Future[None]:
        if num > self.expected or self.finished.done():
            exc = Exception(f"Only expected up to {self.expected} dominoes")
            if not self.finished.done():
                self.finished.set_exception(exc)
            self.finished = self.loop.create_future()
            self.finished.set_exception(exc)
            raise exc

        if num in self.futs:
            return self.futs[num]

        fut = self.loop.create_future()
        self.ctx.tramp.set_future_name(fut, name=f"Domino({num})")
        self.futs[num] = fut

        retrieved = self.loop.create_future()
        self.ctx.tramp.set_future_name(retrieved, name=f"Domino({num}.retrieved")
        self.retrieved[num] = retrieved

        fut.add_done_callback(hp.transfer_result(self.finished, errors_only=True))
        return fut

    def __getitem__(self, num: int) -> asyncio.Future[None]:
        if not self.futs[1].done():
            self.futs[1].set_result(None)
        fut = self.make(num)
        if not self.retrieved[num].done():
            self.retrieved[num].set_result(None)
        return fut
