.. _changelog:

Changelog
=========

.. _release-0.2.0:

0.2.0 - 27 April 2025
    * Did a giant cleanup. This is effectively a different library with some
      similar concepts in comparison to the code that was initially copied into
      this project. Lots has been deleted from that as well or re-imagined
      a little differently.
    * Machinery is now Python3.13+
    * Machinery now passes with strict mypy

.. _release-0.1.1:

0.1.1 - 15 December 2024
    * Removed the hp.InvalidStateError shortcut as python3.10 always has
      ``asyncio.exceptions.InvalidStateError``
    * The asyncio test helpers are now part of the public package

.. _release-0.1.0:

0.1.0 - 5 November 2023
    * A straight copy of photons_app.helpers
