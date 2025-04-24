import asyncio
import contextlib
import contextvars
import dataclasses
import functools
import inspect
import logging
import time
import types
from collections.abc import AsyncGenerator, Callable, Generator, Mapping, Sequence
from typing import TYPE_CHECKING, Protocol, Self, Unpack, cast
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
        self,
        t: FakeTime,
        loop: asyncio.AbstractEventLoop,
        precision: float = 0.1,
        log: logging.Logger | None = None,
    ) -> None:
        self.t = t
        self.loop = loop
        self.precision = precision

        if log is None:
            log = logging.getLogger()
            log.level = logging.INFO

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


class Domino(Protocol):
    def __await__(self) -> Generator[None]: ...

    def add_done_callback(
        self, cb: Callable[[hp.protocols.FutureStatus[None]], None]
    ) -> hp.protocols.FutureCallback[None]: ...

    def done(self) -> bool: ...

    def cancelled(self) -> bool: ...

    def exception(self) -> BaseException | None: ...

    def result(self) -> None: ...

    def set_result(self, value: None) -> None: ...

    def set_exception(self, exc: BaseException) -> None: ...

    def cancel(self) -> None: ...


class FutureDominoes[T_Tramp: hp.protocols.Tramp = hp.protocols.Tramp](Protocol):
    @property
    def ctx(self) -> hp.CTX[T_Tramp]: ...

    @property
    def started(self) -> asyncio.Event: ...

    @property
    def finished(self) -> asyncio.Event: ...

    def begin(self) -> None: ...

    def __getitem__(self, num: int) -> Domino: ...


@dataclasses.dataclass(frozen=True, kw_only=True)
class _Domino[T_Tramp: hp.protocols.Tramp = hp.protocols.Tramp]:
    _i: int
    _ctx: hp.CTX[T_Tramp]
    _fut: asyncio.Future[None]
    _started: asyncio.Event
    _requirements: Sequence[tuple[asyncio.Event, asyncio.Future[None]]]

    def __await__(self) -> Generator[None]:
        return (yield from self._wait().__await__())

    async def _wait(self) -> None:
        await self._started.wait()

        for retrieved, fut in self._requirements:
            await retrieved.wait()
            await self._ctx.wait_for_all_futures(fut)

    def add_done_callback(
        self, cb: Callable[[hp.protocols.FutureStatus[None]], None]
    ) -> hp.protocols.FutureCallback[None]:
        self._fut.add_done_callback(cb)
        return cb

    def done(self) -> bool:
        return self._fut.done()

    def cancelled(self) -> bool:
        return self._fut.cancelled()

    def exception(self) -> BaseException | None:
        return self._fut.exception()

    def result(self) -> None:
        return self._fut.result()

    def set_result(self, value: None) -> None:
        self._fut.set_result(value)

    def set_exception(self, exc: BaseException) -> None:
        self._fut.set_exception(exc)

    def cancel(self) -> None:
        self._fut.cancel()


@dataclasses.dataclass(frozen=True, kw_only=True)
class _FutureDominoes[T_Tramp: hp.protocols.Tramp = hp.protocols.Tramp]:
    ctx: hp.CTX[T_Tramp]
    started: asyncio.Event
    finished: asyncio.Event

    expected: int
    futs: Mapping[int, Domino]
    retrieved: dict[int, asyncio.Event]
    on_retrieved: set[asyncio.Future[None]]

    @classmethod
    def create(
        cls,
        *,
        ctx: hp.CTX[T_Tramp],
        expected: int,
    ) -> Self:
        futs: dict[int, Domino] = {}
        retrieved: dict[int, asyncio.Event] = {}

        started = asyncio.Event()

        requirements: list[tuple[asyncio.Event, asyncio.Future[None]]] = []

        for i in range(1, expected + 1):
            retrieved_event = asyncio.Event()
            retrieved[i] = retrieved_event

            fut: asyncio.Future[None] = ctx.loop.create_future()
            requirements.append((retrieved_event, fut))
            ctx.tramp.set_future_name(fut, name=f"Domino[{i}]")
            futs[i] = _Domino(
                _ctx=ctx,
                _started=started,
                _i=i,
                _requirements=list(requirements),
                _fut=fut,
            )

        on_retrieved_futs: set[asyncio.Future[None]] = set()

        instance = cls(
            ctx=ctx,
            started=started,
            finished=asyncio.Event(),
            expected=expected,
            futs=futs,
            retrieved=retrieved,
            on_retrieved=on_retrieved_futs,
        )

        def finished_on_ctx_done(res: hp.protocols.FutureStatus[None]) -> None:
            instance.finished.set()

        ctx.add_done_callback(finished_on_ctx_done)

        def on_fut_done(i: int, res: hp.protocols.FutureStatus[None]) -> None:
            ctx.tramp.log_info(f"FUTURE_DOMINOES: future {i} done")
            if i == expected:
                ctx.tramp.log_info("FUTURE_DOMINOES: all knocked over")
                instance.finished.set()
                return

            if not futs[i + 1].done():

                def set_next(res: hp.protocols.FutureStatus[None]) -> None:
                    ctx.loop.call_soon(futs[i + 1].set_result, None)

                on_retrieved = ctx.fut_from_event(retrieved[i + 1], name=f"on_retrieved({i + 1})")
                on_retrieved.add_done_callback(set_next)
                on_retrieved_futs.add(on_retrieved)

        for i, domino in futs.items():
            domino.add_done_callback(functools.partial(on_fut_done, i))

        return instance

    def begin(self) -> None:
        self.futs[1].set_result(None)
        self.started.set()

    async def check_finished(self) -> None:
        not_done: set[int] = set()
        for i, fut in self.futs.items():
            if not fut.done():
                not_done.add(i)

        not_retrieved: set[int] = set()
        for i, retrieved in self.retrieved.items():
            if not retrieved.is_set():
                not_retrieved.add(i)

        for domino in self.on_retrieved:
            domino.cancel()

        await self.ctx.wait_for_all_futures(*list(self.on_retrieved))

        if not_retrieved:
            raise AssertionError(f"Not all the futures were accessed: {not_retrieved}")

        if not_done:
            raise AssertionError(f"Not all the futures were completed: {not_done}")

    def __getitem__(self, num: int) -> Domino:
        self.retrieved[num].set()
        return self.futs[num]


@contextlib.asynccontextmanager
async def future_dominoes(
    *,
    expected: int,
    loop: asyncio.AbstractEventLoop | None = None,
    log: logging.Logger | None = None,
) -> AsyncGenerator[FutureDominoes]:
    """
    A helper to start a domino of futures.

    For example:

    .. code-block:: python

        from collections.abc import AsyncGenerator

        from machinery import test_helpers as thp

        async def run() -> None:
            async with thp.future_dominoes(loop=loop, expected=8) as futs:
                called: list[object] = []

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

                loop = ...
                loop.create_task(three())
                loop.create_task(one())

                async def run_two() -> None:
                    async for r in two():
                        called.append(r)

                loop.create_task(run_two())
                futs.begin()
                await futs.finished.wait()

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
    if loop is None:
        loop = asyncio.get_running_loop()

    if log is None:
        log = logging.getLogger()
        log.level = logging.INFO

    tramp: hp.protocols.Tramp = hp.Tramp(log=log)
    ctx = hp.CTX.beginning(loop=loop, name="::", tramp=tramp)

    dominoes = _FutureDominoes.create(ctx=ctx, expected=expected)
    try:
        yield dominoes
    finally:
        await dominoes.check_finished()


if TYPE_CHECKING:
    _D: hp.protocols.FutureStatus[None] = cast(_Domino, None)
