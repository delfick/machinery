import asyncio
import contextlib
import dataclasses
import enum
import functools
import inspect
from collections.abc import (
    AsyncGenerator,
    Callable,
    Coroutine,
    Generator,
    Iterable,
    Iterator,
    Sequence,
)
from typing import Optional

from . import _futures, _protocols, _queue, _task_holder


class QueueInput(enum.Enum):
    """
    An enum of the different source types provided to the feeder
    """

    SYNC_FUNCTION = "SYNC_FUNCTION"
    SYNC_GENERATOR = "SYNC_GENERATOR"
    SYNC_ITERATOR = "SYNC_ITERATOR"
    VALUE = "VALUE"
    COROUTINE = "COROUTINE"
    TASK = "TASK"
    ASYNC_GENERATOR = "ASYNC_GENERATOR"


@dataclasses.dataclass(frozen=True, kw_only=True)
class QueueManagerSuccess[T_QueueContext]:
    """
    These are yielded when we have a value being successfully provided
    by an input source.

    The sources list will be a list of tuples of the source type and the object
    used as that source.

    The context will be the object provided by the first source that led to this
    value
    """

    sources: Sequence[tuple[QueueInput, object]]
    value: object
    context: T_QueueContext


@dataclasses.dataclass(frozen=True, kw_only=True)
class QueueManagerFailure[T_QueueContext]:
    """
    These are yielded when we have an input source raising an exception.

    The sources list will be a list of tuples of the source type and the object
    used as that source.

    The context will be the object provided by the first source that led to this
    value
    """

    sources: Sequence[tuple[QueueInput, object]]
    exception: BaseException
    context: T_QueueContext


@dataclasses.dataclass(frozen=True, kw_only=True)
class QueueManagerIterationStop[T_QueueContext]:
    """
    These are yielded when we have an iteration (either sync or async) reaching
    it's end.

    The sources list will be a list of tuples of the source type and the object
    used as that source.

    The context will be the object provided by the first source that led to this
    value
    """

    sources: Sequence[tuple[QueueInput, object]]
    exception: BaseException | None
    context: T_QueueContext


@dataclasses.dataclass(frozen=True, kw_only=True)
class QueueManagerStopped:
    """
    This is yielded when the queue itself has stopped. If it reached a natural
    end then the exception will be None.
    """

    exception: BaseException | None = None


# This represents the union of types that can be yielded by the streamer
type QueueManagerResult[T_QueueContext] = (
    QueueManagerSuccess[T_QueueContext]
    | QueueManagerFailure[T_QueueContext]
    | QueueManagerIterationStop[T_QueueContext]
    | QueueManagerStopped
)


@dataclasses.dataclass(frozen=True, kw_only=True)
class _QueueSource:
    """
    Represents the source for a value, along with the parent source if there
    was one.

    The finished event should be set when this source has been exhausted.
    """

    input_type: QueueInput
    source: object
    finished: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    parent_source: Optional["_QueueSource"] = None

    @property
    def sources(self) -> Sequence[tuple[QueueInput, object]]:
        result: list[tuple[QueueInput, object]] = [(self.input_type, self.source)]
        if self.parent_source is not None:
            result.extend(self.parent_source.sources)
        return tuple(result)


@dataclasses.dataclass(frozen=True, kw_only=True)
class _QueueFeeder[T_QueueContext, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    """
    Used to implement the ``hp.protocols.QueueFeeder`` interface.
    """

    _ctx: _protocols.CTX[T_Tramp]
    _queue: _protocols.Queue[QueueManagerResult[T_QueueContext]]
    _task_holder: _protocols.TaskHolder
    _make_empty_context: Callable[[], T_QueueContext]

    _sent_stop: asyncio.Event = dataclasses.field(default_factory=asyncio.Event, init=False)
    _sources: list[_QueueSource] = dataclasses.field(default_factory=list, init=False)
    _finished_if_empty_sources: asyncio.Event = dataclasses.field(
        default_factory=asyncio.Event, init=False
    )

    def __post_init__(self) -> None:
        """
        Ensure that we stop the queue when we run out of sources and items.
        """
        self._queue.process_after_yielded(self._process_queue_after_yielded)

    def set_as_finished_if_out_of_sources(self) -> None:
        """
        Set the feeder to understand that when we have no more sources or items
        that we should stop the streamer.
        """
        self._finished_if_empty_sources.set()
        self._clear_sources()

    def _extend_result(
        self, *, result: object, source: _QueueSource, context: T_QueueContext | None
    ) -> None:
        """
        Given some result, either use it as it's own input source, or append to
        the streamer as a successful result.
        """
        match result:
            case Coroutine():
                self.add_coroutine(result, context=context, _parent_source=source)
            case asyncio.Task():
                self.add_task(result, context=context, _parent_source=source)
            case Generator():
                self.add_sync_iterator(result, context=context, _parent_source=source)
            case AsyncGenerator():
                self.add_async_generator(result, context=context, _parent_source=source)
            case _ if callable(result) and len(inspect.signature(result).parameters) == 0:
                # Add the callable as an instruction to the end of the streamer
                # so that we co-operate with other sources
                self._queue.append_instruction(
                    functools.partial(
                        self.add_sync_function, result, context=context, _parent_source=source
                    )
                )
            case _:
                self._queue.append(
                    QueueManagerSuccess(
                        sources=source.sources,
                        value=result,
                        context=context if context is not None else self._make_empty_context(),
                    )
                )

    def add_sync_function(
        self,
        func: Callable[[], object],
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        """
        Add a sync function as a source.

        This is as simple as calling the function and extending the value.

        If the function raises an exception, then we add that failure to the
        streamer.
        """
        source = _QueueSource(
            input_type=QueueInput.SYNC_FUNCTION, source=func, parent_source=_parent_source
        )
        self._sources.append(source)

        try:
            result = func()
        except Exception as exc:
            self._queue.append(
                QueueManagerFailure(
                    sources=source.sources,
                    exception=exc,
                    context=context if context is not None else self._make_empty_context(),
                )
            )
        else:
            self._extend_result(result=result, source=source, context=context)

        source.finished.set()
        self._clear_sources()

    def add_sync_iterator(
        self,
        iterator: Iterable[object] | Iterator[object],
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        """
        Add a sync iterator as an input source.

        We set the source differently depending on whether it's a generator object
        or a normal iterator.

        If it's a normal iterable, we turn it into a generator object before
        continuing.

        Each iteration of the iterable is added as an instruction to the end of
        the streamer to ensure that it is co-operative with the other sources.

        The source is finished when either we get a ``StopIteration`` exception
        or the iterator raises an exception and we will add a
        ``QueueManagerIterationStop`` to the streamer at that point.
        """
        if isinstance(iterator, Generator):
            source = _QueueSource(
                input_type=QueueInput.SYNC_GENERATOR, source=iterator, parent_source=_parent_source
            )
        else:
            if isinstance(iterator, Iterable):
                iterator = iter(iterator)

            source = _QueueSource(
                input_type=QueueInput.SYNC_ITERATOR, source=iterator, parent_source=_parent_source
            )

        self._sources.append(source)

        def on_done(exc: BaseException | None = None) -> None:
            if source.finished.is_set():
                return
            source.finished.set()

            if exc is None and self._ctx.done():
                if self._ctx.cancelled():
                    exc = asyncio.CancelledError()
                else:
                    exc = self._ctx.exception()

            self._queue.append(
                QueueManagerIterationStop(
                    sources=source.sources,
                    exception=exc,
                    context=context if context is not None else self._make_empty_context(),
                )
            )
            source.finished.set()
            self._clear_sources()

        def get_next() -> None:
            if self._ctx.done():
                on_done()
                return

            try:
                nxt = next(iterator)
            except StopIteration:
                on_done()
            except Exception as exc:
                self._queue.append(
                    QueueManagerFailure(
                        sources=source.sources,
                        exception=exc,
                        context=context if context is not None else self._make_empty_context(),
                    )
                )
                on_done(exc)
                self._queue.append_instruction(get_next)
            else:
                # Add each iteration as an instruction to ensure that we co-operate
                # with other sources
                self._queue.append_instruction(
                    functools.partial(
                        self._extend_result, result=nxt, source=source, context=context
                    )
                )
                self._queue.append_instruction(get_next)

        self._queue.append_instruction(get_next)
        self._clear_sources()

    def add_value(
        self,
        value: object,
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        """
        Add a simple value as a source.

        We treat the value as an extendable value.
        """
        source = _QueueSource(
            input_type=QueueInput.VALUE, source=value, parent_source=_parent_source
        )
        self._sources.append(source)

        self._extend_result(result=value, source=source, context=context)
        source.finished.set()
        self._clear_sources()

    def add_coroutine(
        self,
        coro: Coroutine[object, object, object],
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        """
        Add a coroutine as a source.

        We use the loop on the ``ctx`` to create a task and provide that to
        ``add_task``.
        """
        source = _QueueSource(
            input_type=QueueInput.COROUTINE, source=coro, parent_source=_parent_source
        )
        self._sources.append(source)
        self.add_task(self._ctx.loop.create_task(coro), context=context, _parent_source=source)

        source.finished.set()
        self._clear_sources()

    def add_task(
        self,
        task: asyncio.Task[object],
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        """
        Add a task as an input.

        We register the task with our ``task_holder`` so that when the ``ctx``
        finishes, we also finish any tasks that are feeding the streamer.

        We add a done callback to the task such that successful results are
        extended and exceptions are sent as a ``QueueManagerFailure``.
        """
        source = _QueueSource(
            input_type=QueueInput.TASK, source=task, parent_source=_parent_source
        )
        self._sources.append(source)
        self._task_holder.add_task(task)

        def on_done(res: _protocols.FutureStatus[object]) -> None:
            source.finished.set()

            exc: BaseException | None
            if res.cancelled():
                exc = asyncio.CancelledError()
            else:
                exc = res.exception()

            if exc is not None:
                self._queue.append(
                    QueueManagerFailure(
                        sources=source.sources,
                        exception=exc,
                        context=context if context is not None else self._make_empty_context(),
                    )
                )
            else:
                self._extend_result(result=res.result(), source=source, context=context)

            source.finished.set()
            self._clear_sources()

        if task.done():
            on_done(task)
        else:
            task.add_done_callback(on_done)

    def add_async_generator(
        self,
        agen: AsyncGenerator[object],
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        """
        Add an async generator as an input source.

        We take care such that:

        * The generator is closed when the ctx is finished
        * A ``QueueManagerIterationStop`` is added to the stream when the generator
          is exhausted
        * The generator raising an exception results in a ``QueueManagerFailure``
          being added to the queue, and the generator considered exhausted.
        * Values yielded from the generator are extended.
        * Each iteration is processed as an instruction at the end of the queue
          to ensure co-operation with other sources.
        """
        source = _QueueSource(
            input_type=QueueInput.ASYNC_GENERATOR, source=agen, parent_source=_parent_source
        )
        self._sources.append(source)

        def ensure_gen_closed(res: _protocols.FutureStatus[None]) -> None:
            exc: BaseException | None
            if res.cancelled():
                exc = asyncio.CancelledError()
            else:
                exc = res.exception()

            self._task_holder.add_coroutine(_futures.stop_async_generator(agen, exc=exc))

        # Make sure that if the ctx finished before the generator does that we
        # do end up calling ``aclose`` on the generator.
        self._ctx.add_done_callback(ensure_gen_closed)

        def on_done(exc: BaseException | None = None) -> None:
            if source.finished.is_set():
                return

            self._ctx.remove_done_callback(ensure_gen_closed)

            if exc is None and self._ctx.done():
                if self._ctx.cancelled():
                    exc = asyncio.CancelledError()
                else:
                    exc = self._ctx.exception()

            self._queue.append(
                QueueManagerIterationStop(
                    sources=source.sources,
                    exception=exc,
                    context=context if context is not None else self._make_empty_context(),
                )
            )
            source.finished.set()
            self._clear_sources()

        def get_next_instruction() -> None:
            self._task_holder.add_coroutine(get_next())

        async def get_next() -> None:
            try:
                nxt = await agen.__anext__()
            except StopAsyncIteration:
                on_done()
            except Exception as exc:
                self._queue.append(
                    QueueManagerFailure(
                        sources=source.sources,
                        exception=exc,
                        context=context if context is not None else self._make_empty_context(),
                    )
                )
                on_done(exc)

                # Ensure the generator gets to that ``StopAsyncIteration``
                self._queue.append_instruction(get_next_instruction)
            else:
                self._queue.append_instruction(
                    functools.partial(
                        self._extend_result, result=nxt, source=source, context=context
                    )
                )
                self._queue.append_instruction(get_next_instruction)

        self._queue.append_instruction(get_next_instruction)
        self._clear_sources()

    def on_queue_stopped(self, res: _protocols.FutureStatus[None]) -> None:
        """
        This must be registered when this class is created such that it is called
        when the context governing the ctx is complete. It ensures that the
        streamer gets given a ``QueueManagerStopped``.
        """
        exc: BaseException | None
        if res.cancelled():
            exc = asyncio.CancelledError()
        else:
            exc = res.exception()

        self._send_stop(exc, priority=True)

    def _clear_sources(self) -> None:
        """
        Clean up done sources as we go along. This means we don't hold onto
        every single source that gets given to the feeder for a long lived feeder.
        """
        self._sources[:] = [source for source in self._sources if not source.finished.is_set()]

    def _send_stop(self, exc: BaseException | None = None, /, *, priority: bool = False) -> None:
        """
        Ensure we send a ``QueueManagerStopped`` to the streamer.
        """
        if self._sent_stop.is_set():
            return

        self._sent_stop.set()
        self._queue.append(QueueManagerStopped(exception=exc), priority=priority)
        self._queue.breaker.set()

    def _process_queue_after_yielded(
        self, queue: _protocols.LimitedQueue[QueueManagerResult[T_QueueContext]]
    ) -> None:
        """
        We register this such that after a value is yielded from the streamer,
        we see if we should stop after we have empty sources and do so if there are
        no sources and nothing left in the queue to yield.
        """
        if not self._sources and self._finished_if_empty_sources.is_set() and queue.is_empty():
            self._send_stop()


@contextlib.asynccontextmanager
async def queue_manager[T_QueueContext, T_Tramp: _protocols.Tramp = _protocols.Tramp](
    *,
    ctx: _protocols.CTX[T_Tramp],
    make_empty_context: Callable[[], T_QueueContext],
    name: str = "",
) -> AsyncGenerator[
    tuple[
        _protocols.Streamer[QueueManagerResult[T_QueueContext]],
        _protocols.QueueFeeder[T_QueueContext],
    ]
]:
    """
    Create and manager a ``(streamer, feeder)`` pair that can be used to manage
    a stream of values.

    Usage is as follows:

    .. code-block:: python

        from machinery import helpers as hp
        from typing import assert_never

        ctx: hp.CTX = ...

        async with hp.queue_manager(ctx=ctx) as (streamer, feeder):
            feeder.add...
            feeder.add...
            feeder.add...
            feeder.set_as_finished_if_out_of_sources()

            async for result in streamer:
                match result:
                    case hp.QueueManagerSuccess():
                        ...
                    case hp.QueueManagerFailure():
                        ...
                    case hp.QueueManagerIterationStop():
                        ...
                    case hp.QueueManagerStopped():
                        ...
                    case _:
                        assert_never(result)

    The feeder can be added to with a number of methods as found on the
    ``hp.protocols.QueueFeeder`` protocol. These can be used even after the
    ``set_as_finished_if_out_of_sources`` method has been called and at any time.

    The ``set_as_finished_if_out_of_sources`` method says that the streamer will
    not keep waiting when all it's sources are exhausted and nothing is left to
    stream.

    The ``QueueManagerStopped`` will be sent to the queue when the ``ctx`` is
    complete and may be received before the streamer is finished yielding values.

    When values are added to the feeder, a context can be provided that will
    accompany all results.

    To finish a streamer early, ``streamer.breaker.set()`` can be called.

    Values that come from input sources may be "extended" where they are used as
    additional sources rather than added as results.

    These include:

    * callables that only take on argument
    * async generators
    * sync generators
    * Coroutine objects
    * asyncio.Task objects
    """
    with (
        ctx.child(name=f"{name}queue_manager", prefix=name) as ctx_queue_manager,
        ctx_queue_manager.child(name="streamer") as ctx_streamer,
    ):
        async with _task_holder.task_holder(ctx=ctx_queue_manager) as task_holder:
            with _queue.queue(
                ctx=ctx_streamer,
                empty_on_finished=True,
                item_ensurer=_queue.EnsureItemGetter[QueueManagerResult[T_QueueContext]].get(),
            ) as streamer:
                with (
                    ctx_queue_manager.child(name=f"{name}queue_manager[feeder]") as ctx_feeder,
                ):
                    feeder = _QueueFeeder(
                        _ctx=ctx_feeder,
                        _task_holder=task_holder,
                        _queue=streamer,
                        _make_empty_context=make_empty_context,
                    )
                    ctx_streamer.add_done_callback(feeder.on_queue_stopped)
                    yield streamer, feeder
