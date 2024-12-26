import asyncio
import collections
import contextlib
import logging
import os
import queue as stdqueue
import sys
import tempfile
import traceback
import types
from typing import Self

from . import _helpers

log = logging.getLogger("machinery.helpers")

memoized_property = _helpers.memoized_property.memoized_property
ensure_aexit = _helpers.async_mixin.ensure_aexit

stop_async_generator = _helpers.futures.stop_async_generator
fut_to_string = _helpers.futures.fut_to_string
silent_reporter = _helpers.futures.silent_reporter
reporter = _helpers.futures.reporter
get_event_loop = _helpers.futures.get_event_loop
create_future = _helpers.futures.create_future
ResettableFuture = _helpers.futures.ResettableFuture
ChildOfFuture = _helpers.futures.ChildOfFuture
fut_has_callback = _helpers.futures.fut_has_callback
async_as_background = _helpers.futures.async_as_background
async_with_timeout = _helpers.futures.async_with_timeout
wait_for_all_futures = _helpers.futures.wait_for_all_futures
wait_for_first_future = _helpers.futures.wait_for_first_future
cancel_futures_and_wait = _helpers.futures.cancel_futures_and_wait
transfer_result = _helpers.futures.transfer_result
noncancelled_results_from_futs = _helpers.futures.noncancelled_results_from_futs
find_and_apply_result = _helpers.futures.find_and_apply_result

TaskHolder = _helpers.task_holder.TaskHolder

ATicker = _helpers.ticker.ATicker
tick = _helpers.ticker.tick


class Nope:
    """Used to say there was no value"""

    pass


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
        async with ensure_aexit(self):
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
        self.final_future = ChildOfFuture(
            final_future, name=f"ResultStreamer({self.name})::__init__[final_future]"
        )
        self.error_catcher = error_catcher
        self.exceptions_only_to_error_catcher = exceptions_only_to_error_catcher

        self.queue_future = ChildOfFuture(
            final_future, name=f"ResultStreamer({self.name})::__init__[queue_future]"
        )
        self.queue = Queue(
            self.queue_future,
            empty_on_finished=True,
            name=f"ResultStreamer({self.name})::__init__[queue]",
        )

        self.ts = TaskHolder(self.final_future, name=f"ResultStreamer({self.name})::__init__[ts]")

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
                    stop_async_generator(
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
            await cancel_futures_and_wait(
                task, name=f"ResultStreamer({self.name})::add_generator[already_stopped_task]"
            )
            await wait_for_first_future(
                async_as_background(gen.aclose()),
                name=f"ResultStreamer({self.name})::add_generator[already_stopped_gen]",
            )
            return task

        return task

    async def add_coroutine(self, coro, *, context=None, on_done=None, force=False):
        return await self.add_task(
            async_as_background(coro, silent=bool(self.error_catcher)),
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
                await wait_for_all_futures(
                    task, name=f"ResultStreamer({self.name})::add_task[force_already_stopped]"
                )
            else:
                await cancel_futures_and_wait(
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
                        add_error(self.error_catcher, v)

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


@contextlib.contextmanager
def just_log_exceptions(log, *, reraise=None, message="Unexpected error"):
    """
    A context manager that will catch all exceptions and just call::

        log.error(message, exc_info=sys.exc_info())

    Any class in reraise that matches the error will result in re raising the error

    For example:

    .. code-block:: python

        import logging
        import asyncio

        log = logging.getLogger("my_example")

        message = "That's not meant to happen"

        with just_log_exceptions(log, reraise=[asyncio.CancelledError], message=message):
            await do_something()
    """
    try:
        yield
    except Exception as error:
        if reraise and any(isinstance(error, r) for r in reraise):
            raise
        log.error(message, exc_info=sys.exc_info())


def add_error(catcher, error):
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


@contextlib.contextmanager
def a_temp_file():
    """
    Yield the name of a temporary file and ensure it's removed after use

    .. code-block:: python

        with hp.a_temp_file() as fle:
            fle.write("hello")
            fle.flush()
            os.system("cat {0}".format(fle.name))
    """
    filename = None
    tmpfile = None
    try:
        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        filename = tmpfile.name
        yield tmpfile
    finally:
        if tmpfile is not None:
            tmpfile.close()
        if filename and os.path.exists(filename):
            os.remove(filename)


def nested_dict_retrieve(data, keys, dflt):
    """
    Used to get a value deep in a nested dictionary structure

    For example

    .. code-block:: python

        data = {"one": {"two": {"three": "four"}}}

        nested_dict_retrieve(data, ["one", "two", "three"], 6) == "four"

        nested_dict_retrieve(data, ["one", "two"], 6) == {"three": "four"}

        nested_dict_retrieve(data, ["one", "four"], 6) == 6
    """
    if not keys:
        return data

    for key in keys[:-1]:
        if type(data) is not dict:
            return dflt

        if key not in data:
            return dflt

        data = data[key]

    if type(data) is not dict:
        return dflt

    last_key = keys[-1]
    if last_key not in data:
        return dflt

    return data[last_key]


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
        self.final_future = ChildOfFuture(
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
        self.waiter = ResettableFuture(name=f"Queue({self.name})::__init__[waiter]")
        self.collection = collections.deque()
        self.final_future = ChildOfFuture(
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
            await wait_for_first_future(
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
