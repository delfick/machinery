import asyncio
import dataclasses
import enum
from collections.abc import AsyncGenerator, Callable, Coroutine, Iterator, Sequence
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


@dataclasses.dataclass(frozen=True)
class QueueManagerSuccess[T_QueueContext]:
    sources: Sequence[tuple[QueueInput, object]]
    value: object
    context: T_QueueContext


@dataclasses.dataclass(frozen=True)
class QueueManagerFailure[T_QueueContext]:
    sources: Sequence[tuple[QueueInput, object]]
    exception: BaseException
    context: T_QueueContext


@dataclasses.dataclass(frozen=True)
class QueueManagerIterationStop[T_QueueContext]:
    sources: Sequence[tuple[QueueInput, object]]
    exception: BaseException | None
    context: T_QueueContext


@dataclasses.dataclass(frozen=True)
class QueueManagerStopped:
    exception: BaseException | None = None


type QueueManagerResult[T_QueueContext] = (
    QueueManagerSuccess[T_QueueContext]
    | QueueManagerFailure[T_QueueContext]
    | QueueManagerIterationStop[T_QueueContext]
    | QueueManagerStopped
)


@dataclasses.dataclass
class _QueueSource:
    input_type: QueueInput
    source: object
    finished: bool = False
    parent_source: Optional["_QueueSource"] = None

    @property
    def sources(self) -> Sequence[tuple[QueueInput, object]]:
        result: list[tuple[QueueInput, object]] = [(self.input_type, self.source)]
        if self.parent_source is not None:
            result.extend(self.parent_source.sources)
        return tuple(result)


class _QueueFeeder[T_QueueContext, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    def __init__(
        self,
        *,
        ctx: _context.CTX[T_Tramp],
        task_holder: _task_holder.TaskHolder[T_Tramp],
        queue: _queue.Queue[QueueManagerResult[T_QueueContext], T_Tramp],
        make_empty_context: Callable[[], T_QueueContext],
    ) -> None:
        self.ctx = ctx
        self.queue = queue
        self.task_holder = task_holder
        self.sources: list[_QueueSource] = []
        self.make_empty_context = make_empty_context

    def set_as_finished_if_out_of_sources(self) -> None:
        pass

    def add_sync_function(
        self,
        func: Callable[[], object],
        *,
        context: T_QueueContext | None = None,
    ) -> None:
        pass

    def add_sync_iterator(
        self,
        iterator: Iterator[object],
        *,
        context: T_QueueContext | None = None,
    ) -> None:
        pass

    def add_value(
        self,
        value: object,
        *,
        context: T_QueueContext | None = None,
    ) -> None:
        pass

    def add_coroutine(
        self,
        coro: Coroutine[object, object, object],
        *,
        context: T_QueueContext | None = None,
    ) -> None:
        pass

    def add_task(
        self,
        task: asyncio.Task[object],
        *,
        context: T_QueueContext | None = None,
    ) -> None:
        pass

    def add_async_generator(
        self,
        agen: AsyncGenerator[object],
        *,
        context: T_QueueContext | None = None,
    ) -> None:
        pass


class QueueManager[T_QueueContext, T_Tramp: _protocols.Tramp = _protocols.Tramp]:
    def __init__(
        self, *, ctx: _context.CTX[T_Tramp], make_empty_context: Callable[[], T_QueueContext]
    ) -> None:
        self.ctx = ctx
        self.make_empty_context = make_empty_context

    def create_feeder(
        self, *, task_holder: _task_holder.TaskHolder[T_Tramp]
    ) -> tuple[
        _protocols.Streamer[QueueManagerResult[T_QueueContext]],
        _protocols.QueueFeeder[T_QueueContext],
    ]:
        streamer = _queue.Queue[QueueManagerResult[T_QueueContext], T_Tramp](
            ctx=self.ctx, empty_on_finished=True
        )
        feeder = _QueueFeeder(
            ctx=self.ctx,
            task_holder=task_holder,
            queue=streamer,
            make_empty_context=self.make_empty_context,
        )
        return streamer, feeder
