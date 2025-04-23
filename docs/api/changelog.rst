.. _changelog:

Changelog
---------

.. _release-0.2.0:

0.2.0 - TBD
    * Removed ``hp.memoized_property``
    * Removed ``hp.ExceptionGroup`` in favour of python built in ExceptionGroup
    * Removed ``hp.AsyncCMMixin`` in favour of being explicit about what it does
      on classes that use it.
    * Removed ``hp.Nope``
    * Removed a number of other helpers that are too random for this library
    * Removing ``hp.ResultStreamer`` to be replaced with ``hp.QueueFeeder``
    * Renamed ``hp.ATicker`` to ``hp.Ticker``
    * Everything is now in terms of ``ctx: hp.CTX`` rather than ``final_future``
    * Machinery is now Python3.13+

.. _release-0.1.1:

0.1.1 - 15 December 2024
    * Removed the hp.InvalidStateError shortcut as python3.10 always has
      ``asyncio.exceptions.InvalidStateError``
    * The asyncio test helpers are now part of the public package

.. _release-0.1.0:

0.1.0 - 5 November 2023
    * A straight copy of photons_app.helpers
