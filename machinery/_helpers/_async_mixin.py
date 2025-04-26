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
    get called. This is a helper to make it easy to ensure that does happen.

    Usage is as follows:

    .. code-block:: python

        import types

        from machinery import helpers as hp


        class MyCM:
            async def __aenter__(self) -> None:
                async with hp.ensure_aexit(self):
                    return await self.start()

            async def start(self) -> ...:
                ...

            async def __aexit__(
                self,
                exc_typ: type[BaseException] | None,
                value: BaseException | None,
                tb: types.TracebackType | None,
            ) -> None:
                await self.finish(exc_typ, value, tb)

            async def finish(
                self,
                exc_typ: type[BaseException] | None = None,
                value: BaseException | None = None,
                tb: types.TracebackType | None = None,
            ) -> None:
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
