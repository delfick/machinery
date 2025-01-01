import asyncio

import pytest


@pytest.fixture()
def loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_event_loop_policy().get_event_loop()
