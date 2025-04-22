from . import _futures


async def wait_for_all_futures(*futs, name=None):
    """
    Wait for all the futures to be complete and return without error regardless
    of whether the futures completed successfully or not.

    If there are no futures, nothing is done and we return without error.

    We determine all the futures are done when the number of completed futures
    is equal to the number of futures we started with. This is to ensure if a
    future is special and calling done() after the future callback has been
    called is not relevant anymore, we still count the future as done.
    """
    if not futs:
        return

    waiter = _futures.create_future(name=f"||wait_for_all_futures({name})[waiter]")

    unique = {id(fut): fut for fut in futs}.values()
    complete = {}

    def done(res):
        complete[id(res)] = True
        if not waiter.done() and len(complete) == len(unique):
            waiter.set_result(True)

    for fut in unique:
        fut.add_done_callback(done)

    try:
        await waiter
    finally:
        for fut in unique:
            fut.remove_done_callback(done)


async def wait_for_first_future(*futs, name=None):
    """
    Return without error when the first future to be completed is done.
    """
    if not futs:
        return

    waiter = _futures.create_future(name=f"||wait_for_first_future({name})[waiter]")
    unique = {id(fut): fut for fut in futs}.values()

    def done(res):
        if not waiter.done():
            waiter.set_result(True)

    for fut in unique:
        fut.add_done_callback(done)

    try:
        await waiter
    finally:
        for fut in unique:
            fut.remove_done_callback(done)
