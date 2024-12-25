from __future__ import annotations

import contextlib
import sys
from collections.abc import AsyncIterator


def ensure_aexit(
    instance: contextlib.AbstractAsyncContextManager[object],
) -> contextlib.AbstractAsyncContextManager[None]:
    """
    Used to make sure a manual async context manager calls ``__aexit__`` if
    ``__aenter__`` fails.

    Turns out if ``__aenter__`` raises an exception, then ``__aexit__`` doesn't
    get called, which is not how I thought that worked for a lot of context
    managers.

    Usage is as follows:

    .. code-block:: python

        from machinery import helpers as hp


        class MyCM:
            async def __aenter__(self):
                async with hp.ensure_aexit(self):
                    return await self.start()

            async def start(self):
                ...

            async def __aexit__(self, exc_typ, exc, tb):
                await self.finish(exc_typ, exc, tb)

            async def finish(exc_typ=None, exc=None, tb=None):
                ...
    """

    @contextlib.asynccontextmanager
    async def ensure_aexit_cm() -> AsyncIterator[None]:
        exc_info = None
        try:
            yield
        except:
            exc_info = sys.exc_info()

        if exc_info is not None:
            # aexit doesn't run if aenter raises an exception
            await instance.__aexit__(*exc_info)

            exc_t, exc, tb = exc_info
            assert exc_t is not None
            assert exc is not None
            assert tb is not None

            exc.__traceback__ = tb
            raise exc

    return ensure_aexit_cm()
