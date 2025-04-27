.. _async_helpers:

Async Helpers
=============

The ``machinery`` library provides a number of utilities to make it easier to
use asyncio concepts without creating memory leaks or those annoying warnings
that it produces when you have created asyncio tasks that aren't awaited before
the end of the program.

The consistent part of how this works are :class:`machinery.helpers.CTX` objects
that are loosely based of contexts in Go and allow us to create dependency
chains where the parent being completed results in all children contexts also
being completed.

.. autoprotocol:: machinery.helpers.protocols.Tramp
    :member-order: bysource

.. autoprotocol:: machinery.helpers.protocols.CTX
    :member-order: bysource

The ticker
----------

.. autoprotocol:: machinery.helpers.protocols.Ticker

.. autofunction:: machinery.helpers.tick

Task holder
-----------

.. autoprotocol:: machinery.helpers.protocols.TaskHolder

.. autofunction:: machinery.helpers.task_holder

Queues
------

.. autoclass:: machinery.helpers.EnsureItemGetter

.. autoprotocol:: machinery.helpers.protocols.SyncQueue

.. autofunction:: machinery.helpers.sync_queue

.. autoprotocol:: machinery.helpers.protocols.Queue

.. autofunction:: machinery.helpers.queue

Queue Manager
-------------

.. autoprotocol:: machinery.helpers.protocols.QueueFeeder

.. autoprotocol:: machinery.helpers.protocols.Streamer

.. autofunction:: machinery.helpers.queue_manager

.. autoenum:: machinery.helpers.QueueInput

.. autoclass:: machinery.helpers.QueueManagerSuccess

.. autoclass:: machinery.helpers.QueueManagerFailure

.. autoclass:: machinery.helpers.QueueManagerIterationStop

.. autoclass:: machinery.helpers.QueueManagerStopped

Also provided is the type alias for the union of types that the streamer may
produce as ``machinery.helpers.QueueManagerResult``:

QueueManagerResult
++++++++++++++++++

.. code-block:: python

    type QueueManagerResult[T_QueueContext] = (
        QueueManagerSuccess[T_QueueContext]
        | QueueManagerFailure[T_QueueContext]
        | QueueManagerIterationStop[T_QueueContext]
        | QueueManagerStopped
    )

Odd helpers
-----------

There are few standalone helpers for some odd functionality:

.. autofunction:: machinery.helpers.ensure_aexit

.. autofunction:: machinery.helpers.stop_async_generator

.. autofunction:: machinery.helpers.noncancelled_results_from_futs

.. autofunction:: machinery.helpers.find_and_apply_result

And some standalone protocols for some concepts:

.. autoprotocol:: machinery.helpers.protocols.FutureStatus

.. autoprotocol:: machinery.helpers.protocols.FutureCallback

.. autoprotocol:: machinery.helpers.protocols.FutureCTXCallback

.. autoprotocol:: machinery.helpers.protocols.WaitByCallback

.. autoprotocol:: machinery.helpers.protocols.WithRepr
