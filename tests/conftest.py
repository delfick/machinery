import pytest

from machinery import test_helpers as thp


@pytest.fixture(scope="session")
def FutureDominoes():
    return thp.FutureDominoes


@pytest.fixture(scope="session")
def MockedCallLater():
    return thp.MockedCallLater


@pytest.fixture(scope="session")
def FakeTime():
    return thp.FakeTime


@pytest.fixture
def child_future_of():
    return thp.child_future_of


@pytest.fixture
def assertFutCallbacks():
    return thp.assertFutCallbacks
