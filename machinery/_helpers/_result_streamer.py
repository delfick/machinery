import asyncio
import sys
import traceback
import types
from typing import Self

from . import (
    _async_mixin,
    _future_waiters,
    _future_wrappers,
    _futures,
    _queue,
    _task_holder,
)


class ResultStreamer:
    """
    An async generator you can add tasks to and results will be streamed as they
    become available.

    To use this, you first create a streamer and give it a ``final_future`` and
    ``error_catcher``. If the ``final_future`` is cancelled, then the streamer
    will stop and any tasks it knows about will be cancelled.

    The ``error_catcher`` is a standard Machinery error_catcher. If it's a list
    then exceptions will be added to it. If it's a function then it will be
    called with exceptions. Otherwise it is ignored. Note that if you don't
    specify ``exceptions_only_to_error_catcher=True`` then result objects will
    be given to the ``error_catcher`` rather than the exceptions themselves.

    Once you have a streamer you add tasks, coroutines or async generators
    to the streamer. Once you have no more of these to add to the streamer then
    you call ``streamer.no_more_work()`` so that when all remaining tasks have
    finished, the streamer will stop iterating results.

    The streamer will yield ``ResultStreamer.Result`` objects that contain
    the ``value`` from the task, a ``context`` object that you give to the
    streamer when you register a task and a ``successful`` boolean that is
    ``False`` when the result was from an exception.

    When you register a task/coroutine/generator you may specify an ``on_done``
    callback which will be called when it finishes. For tasks and coroutines
    this is called with the result from that task. For async generators it
    is either called with an exception Result if the generator did not exit
    successfully, or a success Result with a ``ResultStreamer.GeneratorComplete``
    instance.

    When you add an async generator, you may specify an ``on_each`` function
    that will be called for each value that is yielded from the generator.

    You may add tasks, coroutines and async generators while you are taking
    results from the streamer.

    For example:

    .. code-block:: python

        from machinery import helpers as hp


        final_future = hp.create_future()

        def error_catcher(error_result):
            print(error_result)

        streamer = hp.ResultStreamer(final_future, error_catcher=error_catcher)
        await streamer.start()

        async def coro_function():
            await something
            await streamer.add_generator(generator2, context=SomeContext())
            return 20

        async def coro_function2():
            return 42

        async def generator():
            for i in range(3):
                yield i
                await something_else
            await streamer.add_coroutine(coro_function2())

        await streamer.add_coroutine(coro_function(), context=20)
        await streamer.add_generator(coro_function())

        async with streamer:
            async for result in streamer:
                print(result.value, result.context, result.successful)

    If you don't want to use the ``async with streamer`` then you must call
    ``await streamer.finish()`` when you are done to ensure everything is
    cleaned up.

    .. autoclass:: machinery.helpers.ResultStreamer.Result

    .. automethod:: add_generator

    .. automethod:: add_coroutine

    .. automethod:: add_value

    .. automethod:: add_task

    .. automethod:: no_more_work

    .. automethod:: finish
    """

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        return await self.finish(exc_type, exc, tb)

    async def __aenter__(self) -> Self:
        async with _async_mixin.ensure_aexit(self):
            return await self.start()

    class GeneratorStopper:
        pass

    class GeneratorComplete:
        pass

    class FinishedTask:
        pass

    class Result:
        """
        The object that the streamer will yield. This contains the ``value``
        being yielded, the ``context`` associated with the coroutine/generator
        this value comes from; and a ``successful`` boolean that says if this
        was an error.
        """

        def __init__(self, value, context, successful):
            self.value = value
            self.context = context
            self.successful = successful

        def __eq__(self, other):
            return (
                isinstance(other, self.__class__)
                and other.value == self.value
                and other.context == self.context
                and other.successful == self.successful
            )

        def __repr__(self):
            status = "success" if self.successful else "failed"
            return f"<Result {status}: {self.value}: {self.context}>"

    def __init__(
        self,
        final_future,
        *,
        error_catcher=None,
        exceptions_only_to_error_catcher=False,
        name=None,
    ):
        self.name = name
        self.final_future = _future_wrappers.ChildOfFuture(
            final_future, name=f"ResultStreamer({self.name})::__init__[final_future]"
        )
        self.error_catcher = error_catcher
        self.exceptions_only_to_error_catcher = exceptions_only_to_error_catcher

        self.queue_future = _future_wrappers.ChildOfFuture(
            final_future, name=f"ResultStreamer({self.name})::__init__[queue_future]"
        )
        self.queue = _queue.Queue(
            self.queue_future,
            empty_on_finished=True,
            name=f"ResultStreamer({self.name})::__init__[queue]",
        )

        self.ts = _task_holder.TaskHolder(
            self.final_future, name=f"ResultStreamer({self.name})::__init__[ts]"
        )

        self._registered = 0
        self.stop_on_completion = False

    async def start(self) -> Self:
        return self

    def __aiter__(self):
        return self.retrieve()

    async def add_generator(self, gen, *, context=None, on_each=None, on_done=None):
        async def run():
            try:
                async for result in gen:
                    result = self.Result(result, context, True)
                    self.queue.append(result)
                    if on_each:
                        on_each(result)
            finally:
                await self.add_coroutine(
                    _futures.stop_async_generator(
                        gen,
                        name=f"ResultStreamer({self.name})::add_generator[stop_gen]",
                        exc=sys.exc_info()[1],
                    ),
                    context=self.GeneratorStopper,
                    force=True,
                )

            return self.GeneratorComplete

        task = await self.add_coroutine(run(), context=context, on_done=on_done)
        task.gen = gen

        if self.final_future.done():
            task.cancel()
            await _future_waiters.wait_for_all_futures(
                task, name=f"ResultStreamer({self.name})::add_generator[already_stopped_task]"
            )
            await _future_waiters.wait_for_first_future(
                _futures.async_as_background(gen.aclose()),
                name=f"ResultStreamer({self.name})::add_generator[already_stopped_gen]",
            )
            return task

        return task

    async def add_coroutine(self, coro, *, context=None, on_done=None, force=False):
        return await self.add_task(
            _futures.async_as_background(coro, silent=bool(self.error_catcher)),
            context=context,
            on_done=on_done,
            force=force,
        )

    async def add_value(self, value, *, context=None, on_done=None):
        async def return_value():
            return value

        return await self.add_coroutine(return_value(), context=context, on_done=on_done)

    async def add_task(self, task, *, context=None, on_done=None, force=False):
        if self.final_future.done():
            if force:
                await _future_waiters.wait_for_all_futures(
                    task, name=f"ResultStreamer({self.name})::add_task[force_already_stopped]"
                )
            else:
                task.cancel()
                await _future_waiters.wait_for_all_futures(
                    task, name=f"ResultStreamer({self.name})::add_task[already_stopped]"
                )
            return task

        def add_to_queue(res):
            successful = False

            if res.cancelled():
                value = asyncio.CancelledError()
            else:
                exc = res.exception()
                if exc:
                    traceback.clear_frames(exc.__traceback__)

                if exc:
                    value = exc
                else:
                    value = res.result()
                    successful = True

            result = self.Result(value, context, successful)

            if value is not self.GeneratorComplete:
                if not successful:
                    v = value if self.exceptions_only_to_error_catcher else result
                    if not isinstance(v, asyncio.CancelledError):
                        self._add_error(self.error_catcher, v)

                self.queue.append(result)

            if on_done:
                on_done(result)

            self.queue.append(self.FinishedTask)

        task.add_done_callback(add_to_queue)
        self.ts.add_task(task)
        self._registered += 1
        return task

    def no_more_work(self):
        self.stop_on_completion = True

    async def retrieve(self):
        if self.stop_on_completion and not self.ts.pending and self._registered == 0:
            return

        self.started = True

        async for nxt in self.queue:
            if nxt is self.FinishedTask:
                self._registered -= 1
            elif nxt.context is self.GeneratorStopper:
                continue
            else:
                yield nxt

            if self.stop_on_completion and not self.ts.pending and self._registered <= 0:
                return

    async def finish(
        self,
        exc_typ: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        self.final_future.cancel()

        try:
            await self.ts.finish(exc_typ, exc, tb)
        finally:
            if not self._registered:
                self.queue_future.cancel()
                await self.queue.finish()

    def _add_error(self, catcher, error):
        """
        Adds an error to an error_catcher.

        This means if it's callable we call it with the error and if it's a ``list``
        or ``set`` we add the error to it.
        """
        if callable(catcher):
            catcher(error)
        elif type(catcher) is list:
            catcher.append(error)
        elif type(catcher) is set:
            catcher.add(error)
