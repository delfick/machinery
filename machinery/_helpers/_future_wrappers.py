from . import _futures


class ResettableFuture:
    """
    A future object with a ``reset()`` function that resets it

    Usage:

    .. code-block:: python

        fut = ResettableFuture()
        fut.set_result(True)
        await fut == True

        fut.reset()
        fut.set_result(False)
        await fut == False

    Calling reset on one of these will do nothing if it already isn't resolved.

    Calling reset on a resolved future will also remove any registered done
    callbacks.
    """

    _asyncio_future_blocking = False

    def __init__(self, name=None):
        self.name = name
        self.fut = _futures.create_future(name=f"ResettableFuture({self.name})::__init__[fut]")

    def reset(self, force=False):
        if force:
            self.fut.cancel()

        if not self.fut.done():
            return

        self.fut = _futures.create_future(name=f"ResettableFuture({self.name})::reset[fut]")

    @property
    def _callbacks(self):
        return self.fut._callbacks

    def set_result(self, data):
        self.fut.set_result(data)

    def set_exception(self, exc):
        self.fut.set_exception(exc)

    def cancel(self):
        self.fut.cancel()

    def result(self):
        return self.fut.result()

    def done(self):
        return self.fut.done()

    def cancelled(self):
        return self.fut.cancelled()

    def exception(self):
        return self.fut.exception()

    def add_done_callback(self, func):
        self.fut.add_done_callback(func)

    def remove_done_callback(self, func):
        self.fut.remove_done_callback(func)

    def __repr__(self):
        return (
            f"<ResettableFuture#{self.name}({_futures.fut_to_string(self.fut, with_name=False)})>"
        )

    def __await__(self):
        return (yield from self.fut)

    __iter__ = __await__


class ChildOfFuture:
    """
    Create a future that also considers the status of it's parent.

    So if the parent is cancelled, then this future is cancelled.
    If the parent raises an exception, then that exception is given to this result

    The special case is if the parent receives a result, then this future is
    cancelled.

    The recommended use is with it's context manager::

        from machinery import helpers as hp

        parent_fut = hp.create_future()

        with hp.ChildOfFuture(parent_fut):
            ...

    If you don't use the context manager then ensure you resolve the future when
    you no longer need it (i.e. ``fut.cancel()``) to avoid a memory leak.
    """

    _asyncio_future_blocking = False

    def __init__(self, original_fut, *, name=None):
        self.name = name
        self.fut = _futures.create_future(name=f"ChildOfFuture({self.name})::__init__[fut]")
        self.original_fut = original_fut

        self.fut.add_done_callback(self.remove_parent_done)
        self.original_fut.add_done_callback(self.parent_done)

    def __enter__(self):
        return self

    def __exit__(self, exc_typ, exc, tb):
        self.cancel()

    def parent_done(self, res):
        if self.fut.done():
            return

        if res.cancelled():
            self.fut.cancel()
            return

        exc = res.exception()
        if exc:
            self.fut.set_exception(exc)
        else:
            self.fut.cancel()

    def remove_parent_done(self, ret):
        self.original_fut.remove_done_callback(self.parent_done)

    @property
    def _callbacks(self):
        return self.fut._callbacks

    def set_result(self, data):
        if self.original_fut.done():
            self.original_fut.set_result(data)
        self.fut.set_result(data)

    def set_exception(self, exc):
        if self.original_fut.done():
            self.original_fut.set_exception(exc)
        self.fut.set_exception(exc)

    def cancel(self):
        self.fut.cancel()

    def result(self):
        if self.original_fut.done() or self.original_fut.cancelled():
            if self.original_fut.cancelled():
                return self.original_fut.result()
            else:
                self.fut.cancel()
        if self.fut.done() or self.fut.cancelled():
            return self.fut.result()
        return self.original_fut.result()

    def done(self):
        return self.fut.done() or self.original_fut.done()

    def cancelled(self):
        if self.fut.cancelled() or self.original_fut.cancelled():
            return True

        # We cancel fut if original_fut gets a result
        if self.original_fut.done() and not self.original_fut.exception():
            self.fut.cancel()
            return True

        return False

    def exception(self):
        if self.fut.done() and not self.fut.cancelled():
            exc = self.fut.exception()
            if exc is not None:
                return exc

        if self.original_fut.done() and not self.original_fut.cancelled():
            exc = self.original_fut.exception()
            if exc is not None:
                return exc

        if self.fut.cancelled() or self.fut.done():
            return self.fut.exception()

        return self.original_fut.exception()

    def cancel_parent(self):
        if hasattr(self.original_fut, "cancel_parent"):
            self.original_fut.cancel_parent()
        else:
            self.original_fut.cancel()

    def add_done_callback(self, func):
        self.fut.add_done_callback(func)

    def remove_done_callback(self, func):
        self.fut.remove_done_callback(func)

    def __repr__(self):
        return f"<ChildOfFuture#{self.name}({_futures.fut_to_string(self.fut, with_name=False)}){_futures.fut_to_string(self.original_fut)}>"

    def __await__(self):
        return (yield from self.fut)

    __iter__ = __await__
