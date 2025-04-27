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
    """
    Represents the type of ``loop.call_later``.
    """

    def __call__[*T_Args](
        self,
        delay: float,
        callback: Callable[[Unpack[T_Args]], object],
        *args: *T_Args,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle: ...


class _CallableWithOriginal(Protocol):
    """
    Represents a callable object that also has a reference to the original
    callable it is wrapping.
    """

    @property
    def original(self) -> Callable[..., object | None]:
        """
        The original function this is wrapping
        """

    def __call__(self) -> None: ...


class Cancellable(Protocol):
    """
    An object that can be cancelled.
    """

    def cancel(self) -> None: ...


class MockedCallLater(Protocol):
    """
    The interface returned by ``thp.mocked_call_later``
    """

    @property
    def called_times(self) -> list[float]:
        """
        A list of times that represent the time callbacks were called.
        """

    async def add(self, amount: float) -> None:
        """
        Process the loop this amount of time, taking care to call any callbacks
        that would be fired if that amount of real time passed, in the order
        that they would.
        """


@dataclasses.dataclass
class _MockedCallLater:
    """
    Implementation for our mocked_call_later helper.
    """

    _ctx: hp.CTX
    _original_call_later: _CallLater

    _time: float
    _precision: float

    funcs: list[tuple[float, _CallableWithOriginal]] = dataclasses.field(default_factory=list)
    called_times: list[float] = dataclasses.field(default_factory=list)
    have_call_later: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)

    def time(self) -> float:
        """
        Return our understanding of what the time is
        """
        return self._time

    async def add(self, amount: float) -> None:
        """
        Run iterations of the loop until we get to ``amount`` more seconds in
        the future.
        """
        await self._run(iterations=round(amount / self._precision))

    async def _main_loop(self) -> None:
        """
        The main loop that ensures that the loop works correctly.

        This must be started by the code that is using this object
        """
        await self.have_call_later.wait()

        while True:
            await self._allow_real_loop()
            await self.have_call_later.wait()
            await self._run()
            if not self.funcs:
                self.have_call_later.clear()

    def _fake_call_later[*T_Args, T_Ret](
        self, when: float, func: Callable[[Unpack[T_Args]], T_Ret], *args: *T_Args
    ) -> Cancellable:
        """
        Our fake implementation of call_later that allows us to pretend to be
        the asyncio loop without letting the passage of time align with the clock.
        """
        fr = inspect.currentframe()
        while fr and "tornado/" not in fr.f_code.co_filename:
            # Tornado gets weird with this helper
            fr = fr.f_back

        if fr:
            # If we have a frame, we are in code we know doesn't work with our
            # helper and we fallback to the real implementation of call later
            return self._original_call_later(when, func, *args)

        # We use the frame to work out where this code comes from
        current_frame = inspect.currentframe()
        assert current_frame is not None
        frame_back = current_frame.f_back
        assert frame_back is not None
        frame_code = frame_back.f_code
        assert frame_code is not None

        called_from = frame_code.co_filename

        # We need to code that originates from alt_pytest_asyncio to
        # use the real loop
        if any(exc in called_from for exc in ("alt_pytest_asyncio/",)):
            return self._original_call_later(when, func, *args)

        # Notify our main loop to wake up
        self.have_call_later.set()

        cancelled = asyncio.Event()

        class Caller:
            """
            It's useful to hold onto the original callback we are setting
            when it comes to debugging weirdness.

            We also want to make sure if the handle is cancelled, that we don't
            actually run the function
            """

            def __init__(s) -> None:
                self.original = func

            def __call__(s) -> None:
                if not cancelled.is_set():
                    self.called_times.append(time.time())
                    func(*args)

        class Handle:
            def cancel(s) -> None:
                cancelled.set()

        # Register a function to be called and when it's mean to be called
        self.funcs.append((round(time.time() + when, 3), Caller()))

        # Return a handle that lets code cancel this call_later
        return Handle()

    async def _allow_real_loop(self, until: float = 0) -> None:
        """
        We need to allow the real loop to run everything that is pending
        during our main loop
        """
        while True:
            ready = self._ctx.loop._ready  # type: ignore[attr-defined]
            ready_len = len(ready)
            await asyncio.sleep(0)
            if ready_len <= until:
                return

    async def _run(self, iterations: int = 0) -> None:
        """
        The main loop is a while loop that keeps calling this.

        We look for all the callables that need to be called later, and progress
        time the ``precision`` amount each iteration and call the callables
        when their time has come.
        """
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
                self._time = round(self._time + self._precision, 3)

        if not executed and iterations == 0:
            self._time = round(self._time + self._precision, 3)


@contextlib.asynccontextmanager
async def mocked_call_later(
    *, ctx: hp.CTX | None = None, precision: float = 0.1, start_time: float = 0, name: str = ""
) -> AsyncGenerator[MockedCallLater]:
    """
    This gets us the ability to wait large periods of time whilst not passing
    very much clock time.

    This works by mocking ``loop.call_later`` so that time in the loop isn't
    in line with time in real life.

    Usage is:

    .. code-block:: python

        from machinery import test_helpers as thp
        from machinery import helpers as hp
        import time

        ctx: hp.CTX = ...


        async with thp.mocked_call_later(ctx=ctx) as m:
            assert time.time() == 0

            event = asyncio.Event()
            ctx.loop.call_later(3, event.set())
            await event.wait()

            assert time.time() == 3 # but in reality effectively no time has passed
    """
    if ctx is None:
        log = logging.getLogger()
        log.level = logging.INFO
        tramp: hp.protocols.Tramp = hp.Tramp(log=log)
        ctx = hp.CTX.beginning(name="::", tramp=tramp)

    with ctx.child(name=f"{name}mocked_call_later", prefix=name) as ctx_mocked:
        instance = _MockedCallLater(
            _ctx=ctx_mocked,
            _original_call_later=ctx.loop.call_later,
            _time=start_time,
            _precision=precision,
        )

        with (
            mock.patch("time.time", instance.time),
            mock.patch.object(ctx.loop, "call_later", instance._fake_call_later),
        ):
            with ctx_mocked.child(name="task_holder") as ctx_task_holder:
                async with hp.task_holder(ctx=ctx_task_holder) as task_holder:
                    task_holder.add_coroutine(instance._main_loop())
                    yield instance
                    ctx_task_holder.cancel()
