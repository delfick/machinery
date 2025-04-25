import asyncio
import collections
import dataclasses
import queue as stdqueue
from collections.abc import AsyncGenerator, Callable, Iterator

from . import _context, _protocols


@dataclasses.dataclass(frozen=True, kw_only=True)
class SyncQueue[T_Item = object, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    A simple wrapper around the standard library non async queue.

    Usage is:

    .. code-block:: python

        from machinery import helpers as hp

        ctx: hp.CTX = ...

        with ctx.child("SyncQueue") as ctx_sync_queue:

            queue = hp.SyncQueue(ctx=ctx_sync_queue)

            async def results():
                for result in queue:
                    print(result)

            ...

            queue.append(something)
            queue.append(another)
    """

    ctx: _context.CTX[T_Tramp]
    timeout: float = 0.05
    empty_on_finished: bool = False

    _collection: stdqueue.Queue[T_Item] = dataclasses.field(
        default_factory=stdqueue.Queue, init=False
    )

    def is_empty(self) -> bool:
        return self._collection.empty()

    def __len__(self) -> int:
        return self._collection.qsize()

    def append(self, item: T_Item) -> None:
        self._collection.put(item)

    def __iter__(self) -> Iterator[T_Item]:
        return iter(self.get_all())

    def get_all(self) -> Iterator[T_Item]:
        while True:
            if self.ctx.done():
                break

            try:
                nxt = self._collection.get(timeout=self.timeout)
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
            if not self._collection.empty():
                yield self._collection.get(block=False)
            else:
                break


@dataclasses.dataclass(frozen=True, kw_only=True)
class Queue[T_Item = object, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    A custom async queue class.

    Usage is:

    .. code-block:: python

        from machinery import helpers as hp

        ctx: hp.CTX = ...

        with ctx.child(name="Queue") as ctx_queue:
            queue = hp.Queue(ctx=ctx_queue)

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

    ctx: _context.CTX[T_Tramp]
    empty_on_finished: bool = False

    _waiter: asyncio.Event = dataclasses.field(default_factory=asyncio.Event, init=False)
    _collection: collections.deque[T_Item] = dataclasses.field(
        default_factory=collections.deque, init=False
    )
    _after_yielded: list[Callable[["Queue[T_Item,T_Tramp]"], None]] = dataclasses.field(
        default_factory=list, init=False
    )

    def __post_init__(self) -> None:
        self.ctx.add_done_callback(self._stop_waiter)

    def _stop_waiter(self, res: _protocols.FutureStatus[None]) -> None:
        self._waiter.set()

    def is_empty(self) -> bool:
        return len(self._collection) == 0

    def __len__(self) -> int:
        return len(self._collection)

    def process_after_yielded(
        self, process: Callable[["Queue[T_Item, T_Tramp]"], None], /
    ) -> None:
        self._after_yielded.append(process)

    def append(self, item: T_Item, *, priority: bool = False) -> None:
        if priority:
            self._collection.insert(0, item)
        else:
            self._collection.append(item)
        self._waiter.set()

    def __aiter__(self) -> AsyncGenerator[T_Item]:
        return self.get_all()

    async def get_all(self) -> AsyncGenerator[T_Item]:
        if not self._collection:
            self._waiter.clear()

        while True:
            task = self.ctx.loop.create_task(self._waiter.wait())
            try:
                await self.ctx.wait_for_first_future(self.ctx, task)
            finally:
                task.cancel()
                await self.ctx.wait_for_all_futures(task)

            if self.ctx.done() and not self.empty_on_finished:
                break

            if self.ctx.done() and not self._collection:
                break

            if not self._collection:
                continue

            nxt = self._collection.popleft()
            if nxt is self.Done:
                break

            if not self._collection:
                self._waiter.clear()

            yield nxt

            for process in self._after_yielded:
                process(self)

    def remaining(self) -> Iterator[T_Item]:
        while self._collection:
            yield self._collection.popleft()
