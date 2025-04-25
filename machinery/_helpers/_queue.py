import asyncio
import collections
import dataclasses
import queue as stdqueue
from collections.abc import AsyncGenerator, Callable, Iterator
from typing import TYPE_CHECKING, cast

from . import _protocols


@dataclasses.dataclass(frozen=True, kw_only=True)
class _SyncQueue[T_Item = object, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    A simple wrapper around the standard library non async queue.

    Usage is:

    .. code-block:: python

        from machinery import helpers as hp

        ctx: hp.CTX = ...

        with ctx.child("SyncQueue") as ctx_sync_queue:

            queue = hp.sync_queue(ctx=ctx_sync_queue)

            async def results():
                for result in queue:
                    print(result)

            ...

            queue.append(something)
            queue.append(another)
    """

    _ctx: _protocols.CTX[T_Tramp]
    _timeout: float = 0.05
    _empty_on_finished: bool = False

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
            if self._ctx.done():
                break

            try:
                nxt = self._collection.get(timeout=self._timeout)
            except stdqueue.Empty:
                continue
            else:
                if self._ctx.done():
                    break

                yield nxt

        if self._ctx.done() and self._empty_on_finished:
            for nxt in self.remaining():
                yield nxt

    def remaining(self) -> Iterator[T_Item]:
        while True:
            if not self._collection.empty():
                yield self._collection.get(block=False)
            else:
                break


@dataclasses.dataclass(frozen=True, kw_only=True)
class _Queue[T_Item = object, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    A custom async queue class.

    Usage is:

    .. code-block:: python

        from machinery import helpers as hp

        ctx: hp.CTX = ...

        with ctx.child(name="Queue") as ctx_queue:
            queue = hp.queue(ctx=ctx_queue)

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

    _ctx: _protocols.CTX[T_Tramp]
    _empty_on_finished: bool = False

    _waiter: asyncio.Event = dataclasses.field(default_factory=asyncio.Event, init=False)
    _collection: collections.deque[T_Item] = dataclasses.field(
        default_factory=collections.deque, init=False
    )
    _after_yielded: list[Callable[["_protocols.LimitedQueue[T_Item]"], None]] = dataclasses.field(
        default_factory=list, init=False
    )

    breaker: asyncio.Event = dataclasses.field(default_factory=asyncio.Event, init=False)

    def __post_init__(self) -> None:
        self._ctx.add_done_callback(self._stop_waiter)

    def _stop_waiter(self, res: _protocols.FutureStatus[None]) -> None:
        self._waiter.set()

    def is_empty(self) -> bool:
        return len(self._collection) == 0

    def __len__(self) -> int:
        return len(self._collection)

    def process_after_yielded(
        self, process: Callable[["_protocols.LimitedQueue[T_Item]"], None], /
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
        self.breaker.clear()

        if not self._collection:
            self._waiter.clear()

        while True:
            wait_task = self._ctx.loop.create_task(self._waiter.wait())
            break_task = self._ctx.loop.create_task(self.breaker.wait())
            try:
                await self._ctx.wait_for_first_future(self._ctx, wait_task, break_task)
            finally:
                wait_task.cancel()
                break_task.cancel()
                await self._ctx.wait_for_all_futures(wait_task, break_task)

            if (self._ctx.done() or self.breaker.is_set()) and not self._empty_on_finished:
                break

            if (self._ctx.done() or self.breaker.is_set()) and not self._collection:
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

    def add_done_callback(
        self, cb: Callable[[_protocols.FutureStatus[None]], None]
    ) -> _protocols.FutureCallback[None]:
        return self._ctx.add_done_callback(cb)


def queue[T_Item = object](
    *, ctx: _protocols.CTX, empty_on_finished: bool = False
) -> _protocols.Queue[T_Item]:
    return _Queue[T_Item](_ctx=ctx, _empty_on_finished=empty_on_finished)


def sync_queue[T_Item = object](
    *,
    ctx: _protocols.CTX,
    timeout: float = 0.05,
    empty_on_finished: bool = False,
) -> _protocols.SyncQueue[T_Item]:
    return _SyncQueue[T_Item](_ctx=ctx, _timeout=timeout, _empty_on_finished=empty_on_finished)


if TYPE_CHECKING:
    _Q: _protocols.Queue[object] = cast(_Queue[object], None)
    _SQ: _protocols.SyncQueue[object] = cast(_SyncQueue[object], None)
