import asyncio
import contextlib
import dataclasses
import enum
import inspect
from collections.abc import AsyncGenerator, Callable, Coroutine, Generator, Iterator, Sequence
from typing import Optional

from . import _context, _protocols, _queue, _task_holder


class QueueInput(enum.Enum):
    SYNC_FUNCTION = "SYNC_FUNCTION"
    SYNC_GENERATOR = "SYNC_GENERATOR"
    SYNC_ITERATOR = "SYNC_ITERATOR"
    VALUE = "VALUE"
    COROUTINE = "COROUTINE"
    TASK = "TASK"
    ASYNC_GENERATOR = "ASYNC_GENERATOR"


@dataclasses.dataclass(frozen=True, kw_only=True)
class QueueManagerSuccess[T_QueueContext]:
    sources: Sequence[tuple[QueueInput, object]]
    value: object
    context: T_QueueContext


@dataclasses.dataclass(frozen=True, kw_only=True)
class QueueManagerFailure[T_QueueContext]:
    sources: Sequence[tuple[QueueInput, object]]
    exception: BaseException
    context: T_QueueContext


@dataclasses.dataclass(frozen=True, kw_only=True)
class QueueManagerIterationStop[T_QueueContext]:
    sources: Sequence[tuple[QueueInput, object]]
    exception: BaseException | None
    context: T_QueueContext


@dataclasses.dataclass(frozen=True, kw_only=True)
class QueueManagerStopped:
    exception: BaseException | None = None


type QueueManagerResult[T_QueueContext] = (
    QueueManagerSuccess[T_QueueContext]
    | QueueManagerFailure[T_QueueContext]
    | QueueManagerIterationStop[T_QueueContext]
    | QueueManagerStopped
)


@dataclasses.dataclass(frozen=True, kw_only=True)
class _QueueSource:
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
    ctx: _context.CTX[T_Tramp]
    queue: _protocols.Queue[QueueManagerResult[T_QueueContext]]
    task_holder: _protocols.TaskHolder
    make_empty_context: Callable[[], T_QueueContext]

    sent_stop: asyncio.Event = dataclasses.field(default_factory=asyncio.Event, init=False)
    sources: list[_QueueSource] = dataclasses.field(default_factory=list, init=False)
    finished_if_empty_sources: asyncio.Event = dataclasses.field(
        default_factory=asyncio.Event, init=False
    )

    def __post_init__(self) -> None:
        self.queue.add_done_callback(self._on_queue_stopped)
        self.queue.process_after_yielded(self._process_queue_after_yielded)

    def set_as_finished_if_out_of_sources(self) -> None:
        self.finished_if_empty_sources.set()
        self._clear_sources()

    def _extend_result(
        self, *, result: object, source: _QueueSource, context: T_QueueContext | None
    ) -> None:
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
                self.add_sync_function(result, context=context, _parent_source=source)
            case _:
                self.queue.append(
                    QueueManagerSuccess(
                        sources=source.sources,
                        value=result,
                        context=context if context is not None else self.make_empty_context(),
                    )
                )

    def add_sync_function(
        self,
        func: Callable[[], object],
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        source = _QueueSource(input_type=QueueInput.SYNC_FUNCTION, source=func)
        self.sources.append(source)

        try:
            result = func()
        except Exception as exc:
            self.queue.append(
                QueueManagerFailure(
                    sources=source.sources,
                    exception=exc,
                    context=context if context is not None else self.make_empty_context(),
                )
            )
        else:
            self._extend_result(result=result, source=source, context=context)

        source.finished.set()
        self._clear_sources()

    def add_sync_iterator(
        self,
        iterator: Iterator[object],
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        if isinstance(iterator, Generator):
            source = _QueueSource(
                input_type=QueueInput.SYNC_GENERATOR, source=iterator, parent_source=_parent_source
            )
        else:
            source = _QueueSource(
                input_type=QueueInput.SYNC_ITERATOR, source=iterator, parent_source=_parent_source
            )

        self.sources.append(source)

        async def process_iterator() -> None:
            while True:
                try:
                    nxt = next(iterator)
                except StopIteration:
                    break
                except Exception as exc:
                    event = asyncio.Event()
                    self.ctx.loop.call_soon(event.set)
                    await event.wait()
                    self.queue.append(
                        QueueManagerFailure(
                            sources=source.sources,
                            exception=exc,
                            context=context if context is not None else self.make_empty_context(),
                        )
                    )
                    break
                else:
                    event = asyncio.Event()
                    self.ctx.loop.call_soon(event.set)
                    await event.wait()
                    self._extend_result(result=nxt, source=source, context=context)

        def on_done(res: _protocols.FutureStatus[None]) -> None:
            source.finished.set()

            exc: BaseException | None
            if res.cancelled():
                exc = asyncio.CancelledError()
            else:
                exc = res.exception()

            self.queue.append(
                QueueManagerIterationStop(
                    sources=source.sources,
                    exception=exc,
                    context=context if context is not None else self.make_empty_context(),
                )
            )
            source.finished.set()
            self._clear_sources()

        task = self.task_holder.add_coroutine(process_iterator())
        task.add_done_callback(on_done)
        self._clear_sources()

    def add_value(
        self,
        value: object,
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        source = _QueueSource(
            input_type=QueueInput.VALUE, source=value, parent_source=_parent_source
        )
        self.sources.append(source)

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
        source = _QueueSource(
            input_type=QueueInput.COROUTINE, source=coro, parent_source=_parent_source
        )
        self.sources.append(source)
        self.add_task(self.task_holder.add_coroutine(coro), context=context, _parent_source=source)

        source.finished.set()
        self._clear_sources()

    def add_task(
        self,
        task: asyncio.Task[object],
        *,
        context: T_QueueContext | None = None,
        _parent_source: _QueueSource | None = None,
    ) -> None:
        source = _QueueSource(
            input_type=QueueInput.TASK, source=task, parent_source=_parent_source
        )
        self.sources.append(source)

        def on_done(res: _protocols.FutureStatus[object]) -> None:
            source.finished.set()

            exc: BaseException | None
            if res.cancelled():
                exc = asyncio.CancelledError()
            else:
                exc = res.exception()

            if exc is not None:
                self.queue.append(
                    QueueManagerFailure(
                        sources=source.sources,
                        exception=exc,
                        context=context if context is not None else self.make_empty_context(),
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
        source = _QueueSource(
            input_type=QueueInput.ASYNC_GENERATOR, source=agen, parent_source=_parent_source
        )
        self.sources.append(source)

        async def process_generator() -> None:
            while True:
                try:
                    nxt = await agen.__anext__()
                except StopAsyncIteration:
                    break
                except Exception as exc:
                    self.queue.append(
                        QueueManagerFailure(
                            sources=source.sources,
                            exception=exc,
                            context=context if context is not None else self.make_empty_context(),
                        )
                    )
                    break
                else:
                    self._extend_result(result=nxt, source=source, context=context)

        def on_done(res: _protocols.FutureStatus[None]) -> None:
            exc: BaseException | None
            if res.cancelled():
                exc = asyncio.CancelledError()
            else:
                exc = res.exception()

            self.queue.append(
                QueueManagerIterationStop(
                    sources=source.sources,
                    exception=exc,
                    context=context if context is not None else self.make_empty_context(),
                )
            )
            source.finished.set()
            self._clear_sources()

        task = self.task_holder.add_coroutine(process_generator())
        task.add_done_callback(on_done)
        self._clear_sources()

    def _on_queue_stopped(self, res: _protocols.FutureStatus[None]) -> None:
        exc: BaseException | None
        if res.cancelled():
            exc = asyncio.CancelledError()
        else:
            exc = res.exception()

        self._send_stop(exc, priority=True)

    def _clear_sources(self) -> None:
        self.sources[:] = [source for source in self.sources if not source.finished.is_set()]

    def _send_stop(self, exc: BaseException | None = None, /, *, priority: bool = False) -> None:
        if self.sent_stop.is_set():
            return

        self.sent_stop.set()
        self.queue.append(QueueManagerStopped(exception=exc), priority=priority)
        self.queue.breaker.set()

    def _process_queue_after_yielded(
        self, queue: _protocols.LimitedQueue[QueueManagerResult[T_QueueContext]]
    ) -> None:
        if not self.sources and self.finished_if_empty_sources.is_set() and queue.is_empty():
            self._send_stop()


@contextlib.asynccontextmanager
async def queue_manager[T_QueueContext, T_Tramp: _protocols.Tramp = _protocols.Tramp](
    *, ctx: _context.CTX[T_Tramp], make_empty_context: Callable[[], T_QueueContext], name: str = ""
) -> AsyncGenerator[
    tuple[
        _protocols.Streamer[QueueManagerResult[T_QueueContext]],
        _protocols.QueueFeeder[T_QueueContext],
    ]
]:
    if name:
        name = f"[{name}]-->"

    def _ensure_queue_manager_result(o: object) -> QueueManagerResult[T_QueueContext]:
        return o  # type: ignore[return-value]

    with ctx.child(name=f"{name}queue_manager") as ctx_queue_manager:
        async with _task_holder.task_holder(ctx=ctx_queue_manager) as task_holder:
            with _queue.queue(
                ctx=ctx_queue_manager,
                empty_on_finished=True,
                item_ensurer=_ensure_queue_manager_result,
            ) as streamer:
                with (
                    ctx_queue_manager.child(name=f"{name}queue_manager[feeder]") as ctx_feeder,
                ):
                    feeder = _QueueFeeder(
                        ctx=ctx_feeder,
                        task_holder=task_holder,
                        queue=streamer,
                        make_empty_context=make_empty_context,
                    )
                    yield streamer, feeder
