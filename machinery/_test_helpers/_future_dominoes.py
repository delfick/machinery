import asyncio
import contextlib
import dataclasses
import functools
import logging
import types
from collections.abc import AsyncGenerator, Callable, Generator, Mapping, Sequence
from typing import TYPE_CHECKING, Protocol, Self, cast

from machinery import helpers as hp


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
    _requirements: Sequence[tuple[asyncio.Future[None], asyncio.Future[None]]]

    def __await__(self) -> Generator[None]:
        return (yield from self._wait().__await__())

    async def _wait(self) -> None:
        await self._started.wait()

        for retrieved, fut in self._requirements:
            await self._ctx.wait_for_all_futures(retrieved, fut)

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
    retrieved: dict[int, asyncio.Future[None]]

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        value: BaseException | None,
        tb: types.TracebackType | None,
    ) -> None:
        self.check_finished()

    @classmethod
    def create(
        cls,
        *,
        ctx: hp.CTX[T_Tramp],
        task_holder: hp.TaskHolder[T_Tramp],
        expected: int,
    ) -> Self:
        futs: dict[int, Domino] = {}
        retrieved: dict[int, asyncio.Future[None]] = {}

        started = asyncio.Event()
        finished = asyncio.Event()

        requirements: list[tuple[asyncio.Future[None], asyncio.Future[None]]] = []

        for i in range(1, expected + 1):
            retrieved_fut = ctx.loop.create_future()
            ctx.tramp.set_future_name(retrieved_fut, name=f"Retrieved({i})")
            retrieved[i] = retrieved_fut

            def announce(i: int, res: hp.protocols.FutureStatus[None]) -> None:
                ctx.tramp.log_info(f"FUTURE_DOMINOES: future {i} retrieved")

            retrieved[i].add_done_callback(functools.partial(announce, i))

            fut: asyncio.Future[None] = ctx.loop.create_future()
            requirements.append((retrieved_fut, fut))
            ctx.tramp.set_future_name(fut, name=f"Domino[{i}]")
            futs[i] = _Domino(
                _ctx=ctx,
                _started=started,
                _i=i,
                _requirements=list(requirements),
                _fut=fut,
            )

        instance = cls(
            ctx=ctx,
            started=started,
            finished=finished,
            expected=expected,
            futs=futs,
            retrieved=retrieved,
        )

        def finished_on_ctx_done(res: hp.protocols.FutureStatus[None]) -> None:
            instance.finished.set()

        ctx.add_done_callback(finished_on_ctx_done)

        async def knock() -> None:
            await started.wait()

            for i, (retrieved, fut) in enumerate(requirements):
                await retrieved
                ctx.loop.call_soon(fut.set_result, None)
                await fut
                ctx.tramp.log_info(f"FUTURE_DOMINOES: future {i + 1} done")
                await instance._allow_real_loop()

            ctx.tramp.log_info("FUTURE_DOMINOES: all knocked over")
            finished.set()

        task_holder.add(knock())
        return instance

    def begin(self) -> None:
        self.started.set()

    def check_finished(self) -> None:
        not_done: set[int] = set()
        for i, fut in self.futs.items():
            if not fut.done():
                not_done.add(i)

        not_retrieved: set[int] = set()
        for i, retrieved in self.retrieved.items():
            if not retrieved.done():
                not_retrieved.add(i)

        if not_retrieved:
            raise AssertionError(f"Not all the futures were accessed: {not_retrieved}")

        if not_done:
            raise AssertionError(f"Not all the futures were completed: {not_done}")

    def __getitem__(self, num: int) -> Domino:
        if not self.retrieved[num].done():
            self.retrieved[num].set_result(None)
        return self.futs[num]

    async def _allow_real_loop(self) -> None:
        while (
            len(
                self.ctx.loop._ready  # type: ignore[attr-defined]
            )
            > 0
        ):
            await asyncio.sleep(0)


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

    with ctx.child(name="task_holder") as ctx_taskholder:
        async with hp.TaskHolder(ctx=ctx_taskholder) as task_holder:
            with ctx_taskholder.child(name="dominoes") as dominoes_ctx:
                with _FutureDominoes.create(
                    ctx=dominoes_ctx, task_holder=task_holder, expected=expected
                ) as dominoes:
                    yield dominoes


if TYPE_CHECKING:
    _D: hp.protocols.FutureStatus[None] = cast(_Domino, None)
