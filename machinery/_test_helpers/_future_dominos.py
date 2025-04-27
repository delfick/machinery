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
    """
    Used to represent the futures provided by FutureDominos.

    A type alias for this exists at ``machinery.test_helpers.Domino``
    """

    def __await__(self) -> Generator[None]:
        """
        Wait for the domino to be knocked over.
        """

    def add_done_callback(
        self, cb: Callable[[hp.protocols.FutureStatus[None]], None]
    ) -> hp.protocols.FutureCallback[None]:
        """
        Add a callback for when the domino is knocked over
        """

    def done(self) -> bool:
        """
        Return true if the domino has been knocked over
        """

    def cancelled(self) -> bool:
        """
        Return true if the domino was cancelled
        """

    def exception(self) -> BaseException | None:
        """
        Return true if the domino was given an exception
        """

    def result(self) -> None:
        """
        Return the result from the domino
        """

    def set_result(self, value: None) -> None:
        """
        Knock over a domino with no exception
        """

    def set_exception(self, exc: BaseException) -> None:
        """
        Set an exception on the domino
        """

    def cancel(self) -> None:
        """
        Set the domino as cancelled (give it an exception of asyncio.CancelledError)
        """


class FutureDominos[T_Tramp: hp.protocols.Tramp = hp.protocols.Tramp](Protocol):
    """
    An object that represents a "domino" set of futures that only complete as
    each previous domino is retrieved and awaited

    A type alias for this exists at ``machinery.test_helpers.FutureDominos``
    """

    @property
    def started(self) -> asyncio.Event:
        """
        This is set for us when the dominos have started
        """

    @property
    def finished(self) -> asyncio.Event:
        """
        This is set for us when all the dominos have been knocked over
        """

    def begin(self) -> None:
        """
        Used by the test to indicate that the dominos should begin
        """

    def __getitem__(self, num: int) -> Domino:
        """
        Get the domino indexed by ``num``
        """


@dataclasses.dataclass(frozen=True, kw_only=True)
class _Domino[T_Tramp: hp.protocols.Tramp = hp.protocols.Tramp]:
    """
    Implementation for each domino
    """

    _i: int
    _ctx: hp.CTX[T_Tramp]
    _fut: asyncio.Future[None]
    _started: asyncio.Event
    _requirements: Sequence[tuple[asyncio.Future[None], asyncio.Future[None]]]

    def __await__(self) -> Generator[None]:
        """
        Wait for the domino to be complete
        """
        return self._wait().__await__()

    async def _wait(self) -> None:
        """
        Implementation for __await__

        We wait for the dominos to begin and then wait for each proceeding
        domino to be retrieved and knocked over.
        """
        await self._started.wait()

        for retrieved, fut in self._requirements:
            await self._ctx.wait_for_all(retrieved, fut)

    def add_done_callback(
        self, cb: Callable[[hp.protocols.FutureStatus[None]], None]
    ) -> hp.protocols.FutureCallback[None]:
        """
        Add a callback to call when the domino is knocked over
        """
        self._fut.add_done_callback(cb)
        return cb

    def done(self) -> bool:
        """
        Return True if the domino is knocked over.
        """
        return self._fut.done()

    def cancelled(self) -> bool:
        """
        Return True if the domino was cancelled.
        """
        return self._fut.cancelled()

    def exception(self) -> BaseException | None:
        """
        Return the exception the future was given if one was

        Raises an exception if the domino isn't knocked over yet
        """
        return self._fut.exception()

    def result(self) -> None:
        """
        Return the result the future was given.

        Raises an exception if the domino wasn't completed with a result.
        """
        return self._fut.result()

    def set_result(self, value: None) -> None:
        """
        Knock over the domino with no exception
        """
        self._fut.set_result(value)

    def set_exception(self, exc: BaseException) -> None:
        """
        Set an exception on the domino
        """
        self._fut.set_exception(exc)

    def cancel(self) -> None:
        """
        Set the domino as cancelled (give it an exception of asyncio.CancelledError)
        """
        self._fut.cancel()


@dataclasses.dataclass(frozen=True, kw_only=True)
class _FutureDominos[T_Tramp: hp.protocols.Tramp = hp.protocols.Tramp]:
    """
    Implementation for our future dominos.
    """

    _ctx: hp.CTX[T_Tramp]

    started: asyncio.Event
    finished: asyncio.Event

    _expected: int
    _futs: Mapping[int, Domino]
    _retrieved: dict[int, asyncio.Future[None]]

    def __enter__(self) -> Self:
        """
        Nothing to do on entering the context manager
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        value: BaseException | None,
        tb: types.TracebackType | None,
    ) -> None:
        """
        Ensure all futures were retrieved and awaited after leaving the
        context manager.
        """
        self.check_finished()

    @classmethod
    def create(
        cls,
        *,
        ctx: hp.CTX[T_Tramp],
        task_holder: hp.protocols.TaskHolder,
        expected: int,
    ) -> Self:
        """
        A classmethod constructor for correctly creating one of these objects.

        We set up all the dominos that will then be managed.
        """
        futs: dict[int, Domino] = {}
        retrieved: dict[int, asyncio.Future[None]] = {}

        started = asyncio.Event()
        finished = asyncio.Event()

        requirements: list[tuple[asyncio.Future[None], asyncio.Future[None]]] = []

        # We make sure that when the dominos are retrieved the first time that
        # is announced.
        # We also create each domino and provide that to our list of futures
        for i in range(1, expected + 1):
            retrieved_fut = ctx.loop.create_future()
            ctx.tramp.set_future_name(retrieved_fut, name=f"{ctx.name}-->[Retrieved({i})]")
            retrieved[i] = retrieved_fut

            def announce(i: int, res: hp.protocols.FutureStatus[None]) -> None:
                ctx.tramp.log_info(f"FUTURE_DOMINOES: future {i} retrieved")

            retrieved[i].add_done_callback(functools.partial(announce, i))

            fut: asyncio.Future[None] = ctx.loop.create_future()
            requirements.append((retrieved_fut, fut))
            ctx.tramp.set_future_name(fut, name=f"{ctx.name}-->[Domino({i})]")
            futs[i] = _Domino(
                _ctx=ctx,
                _started=started,
                _i=i,
                _requirements=list(requirements),
                _fut=fut,
            )

        instance = cls(
            _ctx=ctx,
            started=started,
            finished=finished,
            _expected=expected,
            _futs=futs,
            _retrieved=retrieved,
        )

        def finished_on_ctx_done(res: hp.protocols.FutureStatus[None]) -> None:
            instance.finished.set()

        # Make sure we set finished when we are done
        ctx.add_done_callback(finished_on_ctx_done)

        async def knock() -> None:
            """
            This is what ends up knocking over the dominos.

            Essentially we create a background task that waits for each domino
            to be retrieved and awaited before we then set the next domino as
            done.

            This means that we don't knock over dominos until they have actually
            been retrieved.

            Otherwise later dominos may be marked as complete if they are
            retrieved before earlier dominos.
            """
            await started.wait()

            for i, (retrieved, fut) in enumerate(requirements):
                await retrieved
                fut.set_result(None)
                await fut
                ctx.tramp.log_info(f"FUTURE_DOMINOES: future {i + 1} done")
                await instance._allow_real_loop()

            ctx.tramp.log_info("FUTURE_DOMINOES: all knocked over")
            finished.set()

        task_holder.add_coroutine(knock())
        return instance

    def begin(self) -> None:
        """
        Mark the dominos as ready to starting knocking over
        """
        self.started.set()

    def check_finished(self) -> None:
        """
        Run when the context manager finishes, and ensures that the test
        retrieved and awaited all the dominos.

        Otherwise the test may silently passed even if it didn't use up all
        the dominos!
        """
        not_done: set[int] = set()
        for i, fut in self._futs.items():
            if not fut.done():
                not_done.add(i)

        not_retrieved: set[int] = set()
        for i, retrieved in self._retrieved.items():
            if not retrieved.done():
                not_retrieved.add(i)

        if not_retrieved:
            raise AssertionError(f"Not all the futures were accessed: {not_retrieved}")

        if not_done:
            raise AssertionError(f"Not all the futures were completed: {not_done}")

    def __getitem__(self, num: int) -> Domino:
        """
        Retrieve the domino for this number.

        Note that dominos are 1-indexed and so if we expect 4 dominos then
        that means we expect to retrieve and await ``futs[1]``, ``futs[2]``,
        ``futs[3]`` and ``futs[4]``.
        """
        if not self._retrieved[num].done():
            self._retrieved[num].set_result(None)
        return self._futs[num]

    async def _allow_real_loop(self) -> None:
        """
        We use this when we are knocking over dominos to ensure that as each
        domino is knocked over, anything else on the loop is given time to
        execute before we knock over the next domino.

        Otherwise things can get a little less deterministic.
        """
        while (
            len(
                self._ctx.loop._ready  # type: ignore[attr-defined]
            )
            > 0
        ):
            await asyncio.sleep(0)


@contextlib.asynccontextmanager
async def future_dominos(
    *,
    expected: int,
    loop: asyncio.AbstractEventLoop | None = None,
    log: logging.Logger | None = None,
    name: str = "",
) -> AsyncGenerator[FutureDominos]:
    """
    A helper to start a domino of futures.

    For example:

    .. code-block:: python

        from collections.abc import AsyncGenerator

        from machinery import test_helpers as thp

        async def run() -> None:
            async with thp.future_dominos(loop=loop, expected=8) as futs:
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

    with ctx.child(name="{name}future_dominos", prefix=name) as ctx_future_dominos:
        with (
            ctx_future_dominos.child(name="dominos") as ctx_dominos,
            ctx_future_dominos.child(name="task_holder") as ctx_task_holder,
        ):
            async with hp.task_holder(ctx=ctx_task_holder) as task_holder:
                with _FutureDominos.create(
                    ctx=ctx_dominos, task_holder=task_holder, expected=expected
                ) as dominos:
                    yield dominos
                    ctx_task_holder.cancel()


if TYPE_CHECKING:
    _D: hp.protocols.FutureStatus[None] = cast(_Domino, None)
