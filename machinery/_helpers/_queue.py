import collections
import queue as stdqueue
import types

from . import _future_waiters, _future_wrappers


class SyncQueue:
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

    def __init__(self, final_future, *, timeout=0.05, empty_on_finished=False, name=None):
        self.name = name
        self.timeout = timeout
        self.collection = stdqueue.Queue()
        self.final_future = _future_wrappers.ChildOfFuture(
            final_future, name=f"SyncQueue({self.name})::__init__[final_future]"
        )
        self.empty_on_finished = empty_on_finished

    def append(self, item):
        self.collection.put(item)

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        self.final_future.cancel()

    def __iter__(self):
        return iter(self.get_all())

    def get_all(self):
        while True:
            if self.final_future.done():
                break

            try:
                nxt = self.collection.get(timeout=self.timeout)
            except stdqueue.Empty:
                continue
            else:
                if self.final_future.done():
                    break

                yield nxt

        if self.final_future.done() and self.empty_on_finished:
            for nxt in self.remaining():
                yield nxt

    def remaining(self):
        while True:
            if not self.collection.empty():
                yield self.collection.get(block=False)
            else:
                break


class Queue:
    """
    A custom async queue class.

    Usage is:

    .. code-block:: python

        from machinery import helpers as hp

        final_future = hp.create_future()
        queue = hp.Queue(final_future)

        async def results():
            # This will continue forever until final_future is done
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

    def __init__(self, final_future, *, empty_on_finished=False, name=None):
        self.name = name
        self.waiter = _future_wrappers.ResettableFuture(
            name=f"Queue({self.name})::__init__[waiter]"
        )
        self.collection = collections.deque()
        self.final_future = _future_wrappers.ChildOfFuture(
            final_future, name=f"Queue({self.name})::__init__[final_future]"
        )
        self.empty_on_finished = empty_on_finished

        self.stop = False
        self.final_future.add_done_callback(self._stop_waiter)

    def _stop_waiter(self, res):
        self.waiter.reset()
        self.waiter.set_result(True)

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        self.final_future.cancel()

    def append(self, item):
        self.collection.append(item)
        if not self.waiter.done():
            self.waiter.set_result(True)

    def __aiter__(self):
        return self.get_all()

    async def get_all(self):
        if not self.collection:
            self.waiter.reset()

        while True:
            await _future_waiters.wait_for_first_future(
                self.final_future,
                self.waiter,
                name=f"Queue({self.name})::_get_and_wait[wait_for_next_value]",
            )

            if self.final_future.done() and not self.empty_on_finished:
                break

            if self.final_future.done() and not self.collection:
                break

            if not self.collection:
                continue

            nxt = self.collection.popleft()
            if nxt is self.Done:
                break

            if not self.collection:
                self.waiter.reset()

            yield nxt

    def remaining(self):
        while self.collection:
            yield self.collection.popleft()
