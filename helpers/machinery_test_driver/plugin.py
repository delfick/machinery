import pytest

from machinery import test_helpers as thp


@pytest.fixture(scope="session")
def FutureDominoes() -> type[thp.FutureDominoes]:
    return thp.FutureDominoes


@pytest.fixture(scope="session")
def MockedCallLater() -> type[thp.MockedCallLater]:
    return thp.MockedCallLater


@pytest.fixture(scope="session")
def FakeTime() -> type[thp.FakeTime]:
    return thp.FakeTime


@pytest.fixture
def child_future_of() -> object:
    return thp.child_future_of


@pytest.fixture
def assertFutCallbacks() -> object:
    return thp.assertFutCallbacks
