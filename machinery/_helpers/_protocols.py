from __future__ import annotations

import asyncio
import types
from collections.abc import AsyncGenerator, Callable, Coroutine, Generator, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, Self, cast


class FutureStatus[T_Ret](Protocol):
    """
    Represents a read-only view of a Future
    """

    def done(self) -> bool: ...
    def result(self) -> T_Ret: ...
    def exception(self) -> BaseException | None: ...
    def cancelled(self) -> bool: ...


class FutureCallback[T_Ret](Protocol):
    """
    Represents an object that's used as a done_callback for a future.

    It can be hashed and it is a callable object that takes in the status of
    a completed future.
    """

    def __hash__(self) -> int: ...
    def __call__(self, res: FutureStatus[T_Ret], /) -> None: ...


class FutureCTXCallback[T_Ret, T_Tramp: Tramp = Tramp](Protocol):
    """
    Represents an object that's used as a done_callback for a future but also
    takes in a CTX object.
    """

    def __hash__(self) -> int: ...
    def __call__(self, ctx: CTX[T_Tramp], res: FutureStatus[T_Ret], /) -> None: ...


class WaitByCallback[T_Ret](Protocol):
    """
    Represents an object that supports adding and removing callbacks for when
    the object is done.
    """

    def done(self) -> bool: ...
    def cancel(self) -> bool: ...
    def add_done_callback(
        self, cb: Callable[[FutureStatus[T_Ret]], None]
    ) -> None | FutureCallback[T_Ret]: ...
    def remove_done_callback(self, cb: Callable[[FutureStatus[T_Ret]], None]) -> int: ...


class WithRepr(Protocol):
    """
    Represents an object we can call ``repr`` with.
    """

    def __repr__(self) -> str: ...


class Tramp(Protocol):
    """
    This is an object that is passed around with ``CTX`` objects. It has on it
    the ability to log exceptions and info, as well as the ability to hold onto
    names for futures.

    In python, asyncio.Future objects don't have names and when you have a large
    program with lots of futures hanging around, it becomes very useful to be
    able to name them to understand what they are actually representing.

    The default implementation is provided by :class:`machinery.helpers.Tramp`
    and this can be subclasses to add more methods.

    Note that if you want to use these other methods, then you'll need your ``ctx``
    to be typed as ``hp.protocols.CTX[MyCustomTramp]``.

    It is recommended in your program to have an alias to refer to so that it's
    easy to change the default at a later stage:

    .. code-block:: python

        from machinery import helpers as hp

        type CTX[T_Tramp: MyCustomTramp = MyCustomTramp] = hp.protocols.CTX[T_Tramp]
    """

    def set_future_name(self, fut: asyncio.Future[Any], *, name: str) -> None:
        """
        Given some future, give it a name. This should be done such that the
        name can be retrieved by the sibling ``get_future_name`` function.
        """

    def get_future_name(self, fut: asyncio.Future[Any]) -> str | None:
        """
        Given some future, return it's name as set by ``set_future_name``.

        If no name was set, then return None.
        """

    def log_info(self, msg: str) -> None:
        """
        Log a simple message somewhere. It is up to the implementation to determine
        what that means.
        """

    def log_exception(
        self,
        msg: object,
        *,
        exc_info: tuple[type[BaseException], BaseException, types.TracebackType] | None = None,
    ) -> None:
        """
        Log an exception somewhere. It is up to the implementation to determine
        what that means.
        """

    def fut_to_string(self, f: asyncio.Future[Any] | WithRepr, with_name: bool = True) -> str:
        """
        Given some future, or simply an object with the abiliy to call ``repr``
        on it, return a string representing the future.

        It is up to the implementation to determine how that actually works.

        The default implementation in machinery will report whether the future
        is pending, cancelled, has an exception or has a result. It will also
        provide the name as retrieved from ``get_future_name`` if ``with_name``
        is provided as ``True``.
        """

    @property
    def reporter(self) -> FutureCallback[Any]:
        """
        Return a callable that can be provided as a done callback for a future.

        It is good practice to always give a done callback to a future that looks
        at ``cancelled``, ``exception`` or ``result`` depending on the status
        of the future, to prevent ``asyncio`` from complaining they weren't
        accessed.

        The implementation of this callable should log when the future was
        finished with an exception.
        """

    @property
    def silent_reporter(self) -> FutureCallback[Any]:
        """
        Return a callable that can be provided as a done callback for a future.

        It is good practice to always give a done callback to a future that looks
        at ``cancelled``, ``exception`` or ``result`` depending on the status
        of the future, to prevent ``asyncio`` from complaining they weren't
        accessed.

        The implementation of this callable should **not** log when the future
        was finished with an exception.
        """


class CTX[T_Tramp: Tramp = Tramp](Protocol):
    """
    This object represents a chain of dependency that lets parents in the chain
    cancel children in the chain by cancelling themselves.

    This is loosely based off how contexts work in Go and very loosely based off
    the ideas in languages like erlang with supervisor processes. It is very
    simple and is mainly about ensuring that if a parent is cancelled, so are
    it's children.

    It also contains some useful helpers for working with futures.

    It is good practice for an object that holds onto one of these, to never
    cancel it's own context and instead rely on it's parent to cancel the context
    it is provided.

    So usage looks like:

    .. code-block:: python

        from machinery import helpers as hp

        async def my_program() -> None:
            # The first context is created with ``hp.CTX.beginning()``
            # After this all contexts should be created using ``ctx.child``

            with ctx.child(name="SomeObject") as ctx_some_object:
                some_object = SomeObject(ctx=ctx_some_object)
                # some_object should never itself call ``self.ctx.cancel()``

                with ctx_some_object.child(name=...) as ctx_grandchild:
                    ...

                ...

    Objects that intend to finish should have a mechanism for signalling to itself
    it is done, rather than rely on cancelling the context. Stopping based off
    the ctx being done should only be an indication that the parent wishes to force
    the object to stop what it is doing.

    The context also provides the ability to have additional methods by being
    generic to the ``Tramp``. It is recommended in your own program you create
    a type alias to this class to refer to, so in your code, nothing changes
    if you wish to create a different default type for the tramp.
    """

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """
        The event loop this context is based off.
        """

    @property
    def name(self) -> str:
        """
        The name associated with this context.
        """

    @property
    def tramp(self) -> T_Tramp:
        """
        The tramp allows us to provide the context with additional functionality
        without making the context itself generic.

        The minimum functionality of this object provides the ability to name
        futures and do simple logging.
        """

    def set_exception(self, exc: BaseException) -> None:
        """
        Set an exception on this context and propagate that exception to all
        children contexts.
        """

    def add_on_done(
        self,
        cb: FutureCTXCallback[None, T_Tramp],
        index: FutureCallback[None] | None = None,
    ) -> FutureCallback[None]:
        """
        Register a callback to be called when this context finishes.

        The callback will be provided with both this context and the result of
        the future this context represents.

        To add a done callback that doesn't take in the context, use
        ``add_done_callback``

        If index is provided, then that will be used as the hashable object that
        is recognised by ``remove_done_callback`` to unregister the callback.
        """

    def done(self) -> bool:
        """
        Return True if the future this context represents is completed
        """

    def cancel(self) -> bool:
        """
        Cancel the future this context represents.

        This is a safe no-op if the future is already done.
        """

    def exception(self) -> BaseException | None:
        """
        Get the exception this future was set with if there was one.

        If the future is not complete this will raise an exception as like what
        happens with ``asyncio.Future`` objects.
        """

    def cancelled(self) -> bool:
        """
        Return True if the future represented by this context has been cancelled.
        """

    def add_done_callback(self, cb: Callable[[FutureStatus[None]], None]) -> FutureCallback[None]:
        """
        Register a callback to be called when this future completes.

        If the future is already complete, the callback will be called straight
        away.
        """

    def remove_done_callback(self, cb: Callable[[FutureStatus[None]], None]) -> int:
        """
        Remove the callback from the callbacks on the future represented by this
        callback if that callback was represented.

        Return the number of futures this callback was removed from (a context
        may hold onto many futures)
        """

    def has_direct_done_callback(self, cb: Callable[[FutureStatus[None]], None]) -> bool:
        """
        Return whether this context has this callback registered. If a parent context
        has this callback registered but this one does not, then True will not
        be returned.
        """

    async def wait_for_first(self, *waits: WaitByCallback[Any] | asyncio.Event) -> None:
        """
        Given a number of futures, tasks or events, return when at least one of
        them is complete.

        The default implementation of ``CTX`` will ensure that if any of these
        have been provided and one is already complete, that we will at least
        do an ``await asyncio.sleep(0)`` before returning.
        """

    async def wait_for_all(self, *waits: WaitByCallback[Any] | asyncio.Event) -> None:
        """
        Given a number of futures, tasks or events, return only when all of them
        are complete.

        The default implementation of ``CTX`` will ensure that if any of these
        have been provided and they are all already complete, that we will at least
        do an ``await asyncio.sleep(0)`` before returning.
        """

    async def async_with_timeout[T_Ret](
        self,
        coro: Coroutine[object, object, T_Ret],
        *,
        name: str,
        silent: bool = True,
        timeout: int = 10,
        timeout_event: asyncio.Event | None = None,
        timeout_error: BaseException | None = None,
    ) -> T_Ret:
        """
        Wait for the provided coroutine has completed and either return the result
        from that coroutine, or raise the exception if the coroutine throws an
        exception, or through a timeout exception if the coroutine is still
        running after the provided timeout.

        In the default implementation of ``CTX``, If ``timeout_error`` is provided
        then that error will be raised if the timeout is reached, otherwise the
        coroutine will be sent an ``asyncio.CancelledError()`` and that will be raised.

        The ``timeout_event`` will be set if the timeout is reached before the
        task is complete, otherwise it is never set.

        The default implementation will return only after the task is finished
        cleaning up, which may be some time after the timeout if the task
        catches the ``asyncio.CancelledError`` and does more work.
        """

    def async_as_background[T_Ret](
        self, coro: Coroutine[object, object, T_Ret], *, silent: bool = True
    ) -> asyncio.Task[T_Ret]:
        """
        In the default implementation of ``CTX``, this will create an
        ``asyncio.Task`` from this coroutine and provide either
        ``tramp.reporter`` or ``tramp.silent_reporter`` as a done callback
        depending on the result of ``silent``

        It is up to the user to ensure that this task is awaited at some point
        to avoid asyncio warnings about the task never being awaited.

        For example:

        .. code-block:: python

            task = ctx.async_as_background(my_async_function())
            try:
                await ctx.wait_for_first(task, some_other_event)
            finally:
                task.cancel()
                await ctx.wait_for_all(task)
        """

    def child(self, *, name: str, prefix: str = "") -> Self:
        """
        Create a child context with the provided name and prefix.

        If prefix is provided then the default implementation will set the name
        to be ``[{prefix}]-->{name}``

        The child context will be provided the tramp that is on this context
        and will know about all the futures held by this context.
        """

    def __await__(self) -> Generator[None]:
        """
        Wait for this context to be complete
        """

    def __enter__(self) -> Self:
        """
        Using the context as a context manager will ensure that it is cancelled
        when it goes out of scope.
        """

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        value: BaseException | None,
        tb: types.TracebackType | None,
    ) -> None:
        """
        Ensure the context is cancelled when it goes out of scope.
        """


class TaskHolder(Protocol):
    """
    An object that can be used to create asyncio.Task objects and ensure that
    they are cleaned up correctly without causing asyncio to complain about tasks
    that are never awaited.

    The default implementation is provided by using ``hp.task_holder``:

    .. code-block:: python

        from machinery import helpers as hp

        ctx: hp.CTX = ...

        async with hp.task_holder(ctx=ctx) as ts:
            task = ts.add_coroutine(some_async_function())

    When the context manager is exited then tasks will not be cancelled unless
    the parent ``ctx`` has been cancelled. During this time, more tasks may be
    added to the task holder.

    Once the parent ``ctx`` has been cancelled, then all the tasks that are
    held will be cancelled and awaited.
    """

    def add_coroutine[T_Ret](
        self, coro: Coroutine[object, object, T_Ret], *, silent: bool = False
    ) -> asyncio.Task[T_Ret]:
        """
        Create a task from this coroutine and ensure that it gets cleaned up
        eventually.

        The silent argument should be used to say whether exceptions from this
        coroutine should be logged or not.
        """

    def add_task[T_Ret](self, task: asyncio.Task[T_Ret]) -> asyncio.Task[T_Ret]:
        """
        Track this asyncio.Task object and ensure it is cleaned up correctly
        eventually.
        """

    @property
    def pending(self) -> int:
        """
        Return the number of pending tasks that are held by this object.
        """

    def __contains__(self, task: asyncio.Task[object]) -> bool:
        """
        Return whether this holds onto the provided task.
        """

    def __iter__(self) -> Iterator[WaitByCallback[object]]:
        """
        Yield all the tasks currently held by this object.
        """


class Ticker(Protocol):
    """
    An object that yields on a specific tick schedule.

    The default implementation is found via ``hp.tick``.
    """

    @property
    def pauser(self) -> asyncio.Semaphore | None:
        """
        Pause the ticking whilst this pauser is locked.
        """

    def __aiter__(self) -> AsyncGenerator[tuple[int, float]]:
        """
        Yield according to the tick schedule.

        Each value yielded should be a tuple of ``(iteration, time_till_next)``
        where ``iteration`` is a counter of how many times we yield a value
        starting from 1 and the ``time_till_next`` is the number of seconds till
        the next time we yield a value.
        """

    def change_after(self, every: int, *, set_new_every: bool = True) -> None:
        """
        Change the tick schedule.

        For example `change_after(20)` will mean the next tick is 20 seconds
        after whenever the last tick was.

        Setting ``set_new_every=False`` will mean this is a once only change
        in the schedule, otherwise this new ``every`` will become the new schedule.
        """


class SyncQueue[T_Item = object](Protocol):
    """
    Represents an object that can be used as a synchronous queue.

    The default implementation is via ``hp.sync_queue`` and wraps the standard
    library ``queue.Queue`` class.
    """

    def is_empty(self) -> bool:
        """
        Return true if the queue is currently empty
        """

    def __len__(self) -> int:
        """
        Return how many items are in the queue.
        """

    def append(self, item: T_Item) -> None:
        """
        Add something to the queue
        """

    def __iter__(self) -> Iterator[T_Item]:
        """
        Iterate over the items in the queue. This is re-entrant if the iteration
        is stopped and restarted.
        """

    def get_all(self) -> Iterator[T_Item]:
        """
        Returns an iterator over all the items in the queue. This is re-entrant
        if the iteration is stopped and restarted.
        """

    def remaining(self) -> Iterator[T_Item]:
        """
        Returns an iterator that returns everything that remains in the queue.

        Useful after the queue has stopped and there are still items remaining.

        This iteration will not wait for new items to be added to the queue when
        all the remaining items are yielded
        """


class LimitedQueue[T_Item = object](Protocol):
    """
    A limited Protocol for a machinery Queue that cannot be iterated.

    This is provided to callbacks the queue knows about that get called
    after a value is yielded.

    The methods are the same as those that overlap with Queue.
    """

    @property
    def breaker(self) -> asyncio.Event: ...

    def is_empty(self) -> bool: ...

    def __len__(self) -> int: ...

    def append(self, item: T_Item, *, priority: bool = False) -> None: ...


class QueueItemDef[T_Item](Protocol):
    """
    A callable that given some object will return something of type ``T_Item``.

    These objects are used to ensure that a Queue does yield values of ``T_Item``
    """

    def __call__(self, o: object) -> T_Item: ...


class Queue[T_Item = object](Protocol):
    """
    These objects represent an asynchronous queue of values.
    """

    @property
    def breaker(self) -> asyncio.Event:
        """
        When this is set, the iteration will exit.

        The default implementation will always clear this when iteration is
        restarted.
        """

    def is_empty(self) -> bool:
        """
        Return True if there are no items left in the queue
        """

    def __len__(self) -> int:
        """
        Return the number of items left in the queue.
        """

    def process_after_yielded(self, process: Callable[[LimitedQueue[T_Item]], None], /) -> None:
        """
        Register a callback to be called whenever a value has been yielded.

        The callback will be provided the queue itself and is typed to only
        accept a limited API for that queue.
        """

    def append(self, item: T_Item, *, priority: bool = False) -> None:
        """
        Add an object to the end of the queue.

        If ``priority`` is set to True, then the item is added to the front of
        the queue.
        """

    def append_instruction(self, cb: Callable[[], None], *, priority: bool = False) -> None:
        """
        Add a callable to the end of the queue.

        If ``priority`` is set to True, then the callable is added to the front of
        the queue.

        When the queue gets to these callables, the callable is returned and
        nothing is directly yielded as a result.
        """

    def __aiter__(self) -> AsyncGenerator[T_Item]:
        """
        Asynchronously yield the values in the queue as they are added
        """

    @property
    def get_all(self) -> Callable[[], AsyncGenerator[T_Item]]:
        """
        Return an async generator that yields all the values in the queue
        as they are added.
        """

    def remaining(self) -> Iterator[T_Item]:
        """
        Yield all the remaining values in the queue. This is useful when the queue
        is stopped but there are still values left.

        This will not wait for another value when it runs out of values to yield.
        """


class QueueFeeder[T_QueueContext](Protocol):
    """
    This represents one of the two objects returned by ``hp.queue_manager`` and
    is used to feed items into the streamer it also creates.

    In the default implementation, sources may return "extendable" values which
    will be then used themselves as input sources rather than yielded.

    These include:

    * callables that only take on argument
    * async generators
    * sync generators
    * Coroutine objects
    * asyncio.Task objects

    It does not include normal iterable objects like lists.
    """

    def set_as_finished_if_out_of_sources(self) -> None:
        """
        Calling this will ensure that the streamer stops waiting for more input
        after all sources have dried up.
        """

    def add_sync_function(
        self, func: Callable[[], object], *, context: T_QueueContext | None = None
    ) -> None:
        """
        Register a synchronous function as an input source.

        The value from this function will be provided to the streamer with the
        provided context.

        In the default implementation, an "extendable" result will be used as
        an input source rather than given to the streamer as a value.
        """

    def add_sync_iterator(
        self,
        iterator: Iterable[object] | Iterator[object],
        *,
        context: T_QueueContext | None = None,
    ) -> None:
        """
        Register a synchronous iterator as an input source.

        This can either be a normal Generator object or any other iterable
        object like a list.

        In the default implementation, any "extendable" result will be used as
        an input source rather than given to the streamer as a value.

        In the default implementation, every iteration will be added to the end
        of the streamer as they happen.
        """

    def add_value(self, value: object, *, context: T_QueueContext | None = None) -> None:
        """
        Add a single value to the streamer

        In the default implementation, if the value is "extendable" then it will
        be used as an input source rather than given to the streamer as a value
        """

    def add_coroutine(
        self, coro: Coroutine[object, object, object], *, context: T_QueueContext | None = None
    ) -> None:
        """
        Add a coroutine as a source.

        In the default implementation, this coroutine is used to create an
        asyncio.Task object that is passed into ``add_task``.
        """

    def add_task(
        self, task: asyncio.Task[object], *, context: T_QueueContext | None = None
    ) -> None:
        """
        Add an asyncio.Task as a source.

        In the default implementation, if the result is "extendable" then it will
        be used as an input source rather than given to the streamer as a value
        """

    def add_async_generator(
        self, agen: AsyncGenerator[object], *, context: T_QueueContext | None = None
    ) -> None:
        """
        Add an async generator as a source.

        In the default implementation, any "extendable" result will be used as
        an input source rather than given to the streamer as a value.

        In the default implementation, every iteration will be added to the end
        of the streamer as they happen.
        """


class Streamer[T_Item](Protocol):
    """
    This represents one of the two objects returned by ``hp.queue_manager`` and
    is used to yield the objects that are fed into it by the feeder.
    """

    @property
    def breaker(self) -> asyncio.Event:
        """
        When this event is set, the streamer will finish
        """

    def __aiter__(self) -> AsyncGenerator[T_Item]:
        """
        Yield the values in the streamer as they come in
        """

    def remaining(self) -> Iterator[T_Item]:
        """
        Yield whatever remains in the streamer until no more values are left.

        Useful if the iteration is exited early and there are items left.
        """


if TYPE_CHECKING:
    _FS: FutureStatus[None] = cast(asyncio.Future[None], None)
    _WBC: WaitByCallback[None] = cast(asyncio.Future[None], None)
    cast(asyncio.Future[None], None).add_done_callback(cast(FutureCallback[None], None))
