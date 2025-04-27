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

Odd helpers
-----------

There are few standalone helpers for some odd functionality:

.. autofunction:: machinery.helpers.ensure_aexit

.. autofunction:: machinery.helpers.stop_async_generator

.. autofunction:: machinery.helpers.noncancelled_results_from_futs

.. autofunction:: machinery.helpers.find_and_apply_result
