import asyncio
import collections
import contextlib
import dataclasses
import queue as stdqueue
from collections.abc import AsyncGenerator, Callable, Iterator
from typing import TYPE_CHECKING, cast, overload

from . import _protocols


def _ensure_item[T_Item](o: object) -> T_Item:  # type:ignore[type-var]
    """
    A hack used by EnsureItemGetter to create a QueueItemDef object that
    pretends to ensure the object is of type T_Item
    """
    return o  # type: ignore[return-value]


class EnsureItemGetter[T_Item]:
    """
    Used to return a QueueItemDef object that pretends to ensure that the item
    is ``T_Item``.

    This is useful when we want to create a type safe Queue without the extra
    CPU cycles to ensure that the items added to the queue are indeed of T_Item.

    Usage is:

    .. code-block:: python

        item_ensurer = EnsureItemGetter[MyAmazingType].get()

        reveal_type(item_ensurer) # _protocols.QueueItemDef[MyAmazingType]
    """

    @classmethod
    def get(cls) -> _protocols.QueueItemDef[T_Item]:
        return _ensure_item


@dataclasses.dataclass(frozen=True, kw_only=True)
class _SyncQueue[T_Item = object, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    A wrapper around the standard library ``queue.Queue`` class that implements
    ``hp.protocols.SyncQueue``
    """

    _ctx: _protocols.CTX[T_Tramp]
    _timeout: float = 0.05
    _empty_on_finished: bool = False
    _item_ensurer: _protocols.QueueItemDef[T_Item]

    _collection: stdqueue.Queue[T_Item] = dataclasses.field(
        default_factory=stdqueue.Queue, init=False
    )

    def is_empty(self) -> bool:
        """
        Return True if the queue is empty
        """
        return self._collection.empty()

    def __len__(self) -> int:
        """
        Return the number of items left in the queue
        """
        return self._collection.qsize()

    def append(self, item: T_Item) -> None:
        """
        Append an item to the queue and use ``item_ensurer`` to ensure that the
        item provided is in fact of type ``T_Item``.
        """
        if self._item_ensurer is not _ensure_item:
            item = self._item_ensurer(item)

        self._collection.put(item)

    def __iter__(self) -> Iterator[T_Item]:
        """
        Return an iterator that stays open until the queue is closed.

        If the iterator is exited early, recreating it will be re-entrant.
        """
        return iter(self.get_all())

    def get_all(self) -> Iterator[T_Item]:
        """
        The logic used for ``__iter__``

        If this loop is exited early, it is re-entrant.

        If the queue has ``empty_on_finished`` set to True then it will also
        yield all remaining items in the queue when the context is finished.
        """
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
        """
        Yield all remaining items in the queue until there are non left.
        """
        while True:
            if not self._collection.empty():
                yield self._collection.get(block=False)
            else:
                break


@dataclasses.dataclass(frozen=True, kw_only=True)
class _Instruction:
    """
    Represents a callable that should be called when found during the iteration
    of the queue.

    These are created by calling ``append_instruction`` on the queue.
    """

    cb: Callable[[], None]


@dataclasses.dataclass(frozen=True, kw_only=True)
class _Queue[T_Item, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    An object that can asynchronously iterate values that are added to the queue.
    """

    _ctx: _protocols.CTX[T_Tramp]
    _empty_on_finished: bool = False

    _waiter: asyncio.Event = dataclasses.field(default_factory=asyncio.Event, init=False)
    _collection: collections.deque[T_Item | _Instruction] = dataclasses.field(
        default_factory=collections.deque, init=False
    )
    _after_yielded: list[Callable[["_protocols.LimitedQueue[T_Item]"], None]] = dataclasses.field(
        default_factory=list, init=False
    )
    _item_ensurer: _protocols.QueueItemDef[T_Item]

    breaker: asyncio.Event = dataclasses.field(default_factory=asyncio.Event, init=False)

    def is_empty(self) -> bool:
        """
        Return True if the queue is currently empty
        """
        return len(self) == 0

    def __len__(self) -> int:
        """
        Return the number of items left in the queue
        """
        return len(self._collection)

    def process_after_yielded(
        self, process: Callable[["_protocols.LimitedQueue[T_Item]"], None], /
    ) -> None:
        """
        Register a callable that is provided the queue after a value is yielded
        from the queue.

        The object provided is typed as having a small API surface than the queue
        itself.
        """
        self._after_yielded.append(process)

    def append_instruction(self, cb: Callable[[], None], *, priority: bool = False) -> None:
        """
        Append a callable to the queue, such that when the callable is taken off
        the queue, it is called instead of yielded.

        If ``priority`` is True, then the callable is added to the front of the
        queue.
        """
        if priority:
            self._collection.insert(0, _Instruction(cb=cb))
        else:
            self._collection.append(_Instruction(cb=cb))
        self._waiter.set()

    def append(self, item: T_Item, *, priority: bool = False) -> None:
        """
        Append an item to the queue.

        The ``item_ensurer`` on the class is used to ensure that the item provided
        is of type ``T_Item``. Note that if that object comes from ``EnsureItemGetter``
        then it is not called and the item is assumed to be the correct type.

        If ``priority`` is True, then the callable is added to the front of the
        queue.
        """
        if self._item_ensurer is not _ensure_item:
            item = self._item_ensurer(item)

        if priority:
            self._collection.insert(0, item)
        else:
            self._collection.append(item)
        self._waiter.set()

    def __aiter__(self) -> AsyncGenerator[T_Item]:
        """
        Yield all the items in the queue as they come in.

        Do not exit until the context is finished, or ``breaker`` is set.

        If this is exited early, it is re-entrant.
        """
        return self.get_all()

    async def get_all(self) -> AsyncGenerator[T_Item]:
        """
        Yield all the items in the queue as they come in.

        This is the logic used by ``__aiter__``.

        Do not exit until the context is complete, or ``breaker`` is set.

        If the context is complete or breaker is set, then we will also yield
        what is left in the queue if ``empty_on_finished`` is True.

        If this is exited early, it is re-entrant.
        """
        self.breaker.clear()

        if not self._collection:
            self._waiter.clear()

        while True:
            await self._ctx.wait_for_first(self._ctx, self._waiter, self.breaker)

            if (self._ctx.done() or self.breaker.is_set()) and not self._empty_on_finished:
                break

            if (self._ctx.done() or self.breaker.is_set()) and not self._collection:
                break

            if not self._collection:
                self._waiter.clear()
                continue

            nxt = self._collection.popleft()

            if isinstance(nxt, _Instruction):
                nxt.cb()
            else:
                yield nxt

                for process in self._after_yielded:
                    process(self)

    def remaining(self) -> Iterator[T_Item]:
        """
        Iterate all remaining items in the queue until none are left.
        """
        while self._collection:
            nxt = self._collection.popleft()
            if isinstance(nxt, _Instruction):
                nxt.cb()
            else:
                yield nxt


@overload
def _queue(
    *,
    ctx: _protocols.CTX,
    empty_on_finished: bool = False,
    name: str = "",
    item_ensurer: None = None,
) -> Iterator[_protocols.Queue[object]]: ...


@overload
def _queue[T_Item](
    *,
    ctx: _protocols.CTX,
    empty_on_finished: bool = False,
    name: str = "",
    item_ensurer: _protocols.QueueItemDef[T_Item],
) -> Iterator[_protocols.Queue[T_Item]]: ...


def _queue[T_Item](
    *,
    ctx: _protocols.CTX,
    empty_on_finished: bool = False,
    name: str = "",
    item_ensurer: _protocols.QueueItemDef[T_Item] | None = None,
) -> Iterator[_protocols.Queue[T_Item]] | Iterator[_protocols.Queue[object]]:
    """
    Returns an object that can asynchronously yield the values it gets given.

    Usage is:

    .. code-block:: python

        from machinery import helpers as hp

        ctx: hp.CTX = ...

        with hp.queue(ctx=ctx_queue) as queue:

            async def results():
                # This will continue forever until ctx is done
                async for result in queue:
                    print(result)

            ...

            queue.append(something)
            queue.append(another)

    Note that the main difference between this and the standard library
    asyncio.Queue other than a slighly different API surface, is that this one
    does not have the ability to impose limits.
    """
    with ctx.child(name=f"{name}queue", prefix=name) as ctx_queue:
        if item_ensurer is None:
            yield _Queue(
                _ctx=ctx_queue,
                _empty_on_finished=empty_on_finished,
                _item_ensurer=EnsureItemGetter[object].get(),
            )
        else:
            yield _Queue(
                _ctx=ctx_queue, _empty_on_finished=empty_on_finished, _item_ensurer=item_ensurer
            )


@overload
def _sync_queue(
    *,
    ctx: _protocols.CTX,
    timeout: float = 0.05,
    empty_on_finished: bool = False,
    name: str = "",
    item_ensurer: None = None,
) -> Iterator[_protocols.SyncQueue[object]]: ...


@overload
def _sync_queue[T_Item](
    *,
    ctx: _protocols.CTX,
    timeout: float = 0.05,
    empty_on_finished: bool = False,
    name: str = "",
    item_ensurer: _protocols.QueueItemDef[T_Item],
) -> Iterator[_protocols.SyncQueue[T_Item]]: ...


def _sync_queue[T_Item = object](
    *,
    ctx: _protocols.CTX,
    timeout: float = 0.05,
    empty_on_finished: bool = False,
    name: str = "",
    item_ensurer: _protocols.QueueItemDef[T_Item] | None = None,
) -> Iterator[_protocols.SyncQueue[T_Item]] | Iterator[_protocols.SyncQueue[object]]:
    """
    A simple wrapper around the standard library non async queue.

    Usage is:

    .. code-block:: python

        from machinery import helpers as hp

        ctx: hp.CTX = ...

        with hp.sync_queue(ctx=ctx) as sync_queue:
            async def results():
                for result in sync_queue:
                    print(result)

            ...

            sync_queue.append(something)
            sync_queue.append(another)

    If ``empty_on_finished`` is set to True, then the queue will keep yielding
    what items remain after ``ctx`` is complete.

    The ``item_ensurer`` can be passed in as a function that takes a single ``object``
    and returns an object matching ``T_Item``. This allows us to ensure the return
    type is a Queue that yields ``T_Item`` objects. If there is a strong guarantee
    that the objects provided to the queue will always be the correct type then
    ``hp.EnsureItemGetter`` can be used to return an object for ``item_ensurer``
    that is typed as returning ``T_Item`` but will never be executed by the queue.

    If no ``item_ensurer`` is provided then the queue will be yielding objects
    of type ``object``.

    If a ``timeout`` is provided, then that time out will be used when waiting
    for a new result on the standard library ``Queue.get(timeout=...)``
    """
    with ctx.child(name=f"{name}sync_queue", prefix=name) as ctx_sync_queue:
        if item_ensurer is None:
            yield _SyncQueue(
                _ctx=ctx_sync_queue,
                _timeout=timeout,
                _empty_on_finished=empty_on_finished,
                _item_ensurer=EnsureItemGetter[object].get(),
            )
        else:
            yield _SyncQueue(
                _ctx=ctx_sync_queue,
                _timeout=timeout,
                _empty_on_finished=empty_on_finished,
                _item_ensurer=item_ensurer,
            )


queue = contextlib.contextmanager(_queue)
sync_queue = contextlib.contextmanager(_sync_queue)

if TYPE_CHECKING:
    _Q: _protocols.Queue[object] = cast(_Queue[object], None)
    _SQ: _protocols.SyncQueue[object] = cast(_SyncQueue[object], None)
