.. _pytest_helpers:

Pytest Helpers
==============

Machinery also offers some helpers for working with asyncio in pytest. It is
also recommended to use `alt-pytest-asyncio <https://pypi.org/project/alt-pytest-asyncio/>`_
to write asyncio tests in pytest, but
`pytest-asyncio <https://pypi.org/project/pytest-asyncio/>`_ should also work fine.

Future Dominoes
---------------

.. autoprotocol:: machinery._test_helpers._future_dominos.Domino

.. autoprotocol:: machinery._test_helpers._future_dominos.FutureDominos

.. autofunction:: machinery.test_helpers.future_dominos

Mocked call later
-----------------

.. autoprotocol:: machinery._test_helpers._mocked_call_later.Cancellable

.. autoprotocol:: machinery._test_helpers._mocked_call_later.MockedCallLater

.. autofunction:: machinery.test_helpers.mocked_call_later
