import asyncio
import collections
import queue as stdqueue
import types

from . import _context, _protocols


class SyncQueue[T_Tramp: _protocols.Tramp = _protocols.Tramp]:
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
        self, *, ctx: _context.CTX[T_Tramp], timeout=0.05, empty_on_finished=False, name=None
    ):
        self.name = name
        self.timeout = timeout
        self.collection: stdqueue.Queue = stdqueue.Queue()
        self.ctx = ctx.child(name=f"SyncQueue({self.name})::__init__[ctx]")
        self.empty_on_finished = empty_on_finished

    def append(self, item):
        self.collection.put(item)

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        self.ctx.cancel()

    def __iter__(self):
        return iter(self.get_all())

    def get_all(self):
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

    def remaining(self):
        while True:
            if not self.collection.empty():
                yield self.collection.get(block=False)
            else:
                break


class Queue[T_Tramp: _protocols.Tramp = _protocols.Tramp]:
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

    def __init__(self, *, ctx: _context.CTX[T_Tramp], empty_on_finished=False, name=None):
        self.name = name
        self.collection: collections.deque = collections.deque()
        self.ctx = ctx.child(name=f"Queue({self.name})::__init__[ctx]")
        self.waiter = asyncio.Event()
        self.empty_on_finished = empty_on_finished

        self.stop = False
        self.ctx.add_done_callback(self._stop_waiter)

    def _stop_waiter(self, res):
        self.waiter.set()

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        self.ctx.cancel()

    def append(self, item):
        self.collection.append(item)
        self.waiter.set()

    def __aiter__(self):
        return self.get_all()

    async def get_all(self):
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

    def remaining(self):
        while self.collection:
            yield self.collection.popleft()
