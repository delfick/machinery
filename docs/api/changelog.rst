.. _changelog:

Changelog
---------

.. _release-0.2.0:

0.2.0 - TBD
    * Improved hp.memoized_property and changed it so that it requires
      an explicit ``_memoized_cache`` property on the class to contain
      the cached information.
    * Removed hp.ExceptionGroup in favour of python built in ExceptionGroup

.. _release-0.1.1:

0.1.1 - 15 December 2024
    * Removed the hp.InvalidStateError shortcut as python3.10 always has
      ``asyncio.exceptions.InvalidStateError``
    * The asyncio test helpers are now part of the public package

.. _release-0.1.0:

0.1.0 - 5 November 2023
    * A straight copy of photons_app.helpers
