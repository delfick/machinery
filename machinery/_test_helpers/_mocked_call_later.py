import asyncio
import contextlib
import contextvars
import dataclasses
import inspect
import logging
import time
from collections.abc import AsyncGenerator, Callable
from typing import Protocol, Unpack
from unittest import mock

from machinery import helpers as hp


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


class MockedCallLater(Protocol):
    @property
    def called_times(self) -> list[float]: ...

    def add_time(self, t: float) -> None: ...

    def set_time(self, t: float) -> None: ...

    async def add(self, amount: float) -> None: ...

    async def resume_after(self, amount: float) -> None: ...


@dataclasses.dataclass
class _MockedCallLater:
    _ctx: hp.CTX
    _original_call_later: _CallLater

    _time: float
    _precision: float

    funcs: list[tuple[float, _CallableWithOriginal]] = dataclasses.field(default_factory=list)
    called_times: list[float] = dataclasses.field(default_factory=list)
    have_call_later: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)

    def time(self) -> float:
        return self._time

    def add_time(self, t: float) -> None:
        self._time = round(self._time + t, 3)

    def set_time(self, t: float) -> None:
        self._time = t

    async def add(self, amount: float) -> None:
        await self._run(iterations=round(amount / 0.1))

    async def resume_after(self, amount: float) -> None:
        event = asyncio.Event()
        self._ctx.loop.call_later(amount, event.set)
        await event.wait()

    async def run(self) -> None:
        await self.have_call_later.wait()

        while True:
            await self._allow_real_loop()
            await self.have_call_later.wait()
            await self._run()
            if not self.funcs:
                self.have_call_later.clear()

    def call_later[*T_Args, T_Ret](
        self, when: float, func: Callable[[Unpack[T_Args]], T_Ret], *args: *T_Args
    ) -> Cancellable:
        fr = inspect.currentframe()
        while fr and "tornado/" not in fr.f_code.co_filename:
            fr = fr.f_back
        if fr:
            return self._original_call_later(when, func, *args)

        current_frame = inspect.currentframe()
        assert current_frame is not None
        frame_back = current_frame.f_back
        assert frame_back is not None
        frame_code = frame_back.f_code
        assert frame_code is not None

        called_from = frame_code.co_filename
        if any(exc in called_from for exc in ("alt_pytest_asyncio/",)):
            return self._original_call_later(when, func, *args)

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
            ready = self._ctx.loop._ready  # type: ignore[attr-defined]
            ready_len = len(ready)
            await asyncio.sleep(0)
            if ready_len <= until:
                return

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
                self.add_time(self._precision)

        if not executed and iterations == 0:
            self.add_time(self._precision)

        return executed


@contextlib.asynccontextmanager
async def mocked_call_later(
    *, ctx: hp.CTX | None = None, precision: float = 0.1, start_time: float = 0, name: str = ""
) -> AsyncGenerator[MockedCallLater]:
    if ctx is None:
        log = logging.getLogger()
        log.level = logging.INFO
        tramp: hp.protocols.Tramp = hp.Tramp(log=log)
        ctx = hp.CTX.beginning(name="::", tramp=tramp)

    if name:
        name = f"[{name}]-->"

    with ctx.child(name=f"{name}mocked_call_later") as ctx_mocked:
        instance = _MockedCallLater(
            _ctx=ctx_mocked,
            _original_call_later=ctx.loop.call_later,
            _time=start_time,
            _precision=precision,
        )

        with (
            mock.patch("time.time", instance.time),
            mock.patch.object(ctx.loop, "call_later", instance.call_later),
        ):
            with ctx_mocked.child(name=f"{name}mocked_call_later[task_holder]") as ctx_task_holder:
                async with hp.TaskHolder(ctx=ctx_task_holder) as task_holder:
                    task_holder.add(instance.run())
                    yield instance
                    ctx_task_holder.cancel()
