import asyncio
import collections
import queue as stdqueue
import types
from collections.abc import AsyncGenerator, Callable, Iterator

from . import _context, _protocols


class SyncQueue[T_Item = object, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    A simple wrapper around the standard library non async queue.

    Usage is:

    .. code-block:: python

        from machinery import helpers as hp

        queue = hp.SyncQueue()

        async def results():
            for result in queue:
                print(result)

        ...

        queue.append(something)
        queue.append(another)
    """

    def __init__(
        self,
        *,
        ctx: _context.CTX[T_Tramp],
        timeout: float = 0.05,
        empty_on_finished: bool = False,
        name: str | None = None,
    ) -> None:
        self.name = name
        self.timeout = timeout
        self.collection: stdqueue.Queue[T_Item] = stdqueue.Queue()
        self.ctx = ctx.child(name=f"SyncQueue({self.name})::__init__[ctx]")
        self.empty_on_finished = empty_on_finished

    def append(self, item: T_Item) -> None:
        self.collection.put(item)

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        self.ctx.cancel()

    def __iter__(self) -> Iterator[T_Item]:
        return iter(self.get_all())

    def get_all(self) -> Iterator[T_Item]:
        while True:
            if self.ctx.done():
                break

            try:
                nxt = self.collection.get(timeout=self.timeout)
            except stdqueue.Empty:
                continue
            else:
                if self.ctx.done():
                    break

                yield nxt

        if self.ctx.done() and self.empty_on_finished:
            for nxt in self.remaining():
                yield nxt

    def remaining(self) -> Iterator[T_Item]:
        while True:
            if not self.collection.empty():
                yield self.collection.get(block=False)
            else:
                break


class Queue[T_Item = object, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    A custom async queue class.

    Usage is:

    .. code-block:: python

        from machinery import helpers as hp

        ctx: hp.CTX = ...
        queue = hp.Queue(ctx=ctx)

        async def results():
            # This will continue forever until ctx is done
            async for result in queue:
                print(result)

        ...

        queue.append(something)
        queue.append(another)

    Note that the main difference between this and the standard library
    asyncio.Queue is that this one does not have the ability to impose limits.
    """

    class Done:
        pass

    def __init__(
        self,
        *,
        ctx: _context.CTX[T_Tramp],
        empty_on_finished: bool = False,
        name: str | None = None,
    ) -> None:
        self.name = name
        self.collection: collections.deque[T_Item] = collections.deque()
        self.ctx = ctx.child(name=f"Queue({self.name})::__init__[ctx]")
        self.waiter = asyncio.Event()
        self.empty_on_finished = empty_on_finished
        self.after_yielded: list[Callable[[Queue[T_Item, T_Tramp]], None]] = []

        self.stop = False
        self.ctx.add_done_callback(self._stop_waiter)

    def _stop_waiter(self, res: _protocols.FutureStatus[None]) -> None:
        self.waiter.set()

    def process_after_yielded(
        self, process: Callable[["Queue[T_Item, T_Tramp]"], None], /
    ) -> None:
        self.after_yielded.append(process)

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        self.ctx.cancel()

    def append(self, item: T_Item, *, priority: bool = False) -> None:
        if priority:
            self.collection.insert(0, item)
        else:
            self.collection.append(item)
        self.waiter.set()

    def __aiter__(self) -> AsyncGenerator[T_Item]:
        return self.get_all()

    async def get_all(self) -> AsyncGenerator[T_Item]:
        if not self.collection:
            self.waiter.clear()

        while True:
            await self.ctx.wait_for_first_future(
                self.ctx, self.ctx.fut_from_event(self.waiter, name="get_all")
            )

            if self.ctx.done() and not self.empty_on_finished:
                break

            if self.ctx.done() and not self.collection:
                break

            if not self.collection:
                continue

            nxt = self.collection.popleft()
            if nxt is self.Done:
                break

            if not self.collection:
                self.waiter.clear()

            yield nxt

            for process in self.after_yielded:
                process(self)

    def remaining(self) -> Iterator[T_Item]:
        while self.collection:
            yield self.collection.popleft()
