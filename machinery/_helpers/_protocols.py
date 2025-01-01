from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol, cast


class FutureStatus[T_Ret](Protocol):
    def done(self) -> bool: ...
    def result(self) -> T_Ret: ...
    def exception(self) -> BaseException | None: ...
    def cancelled(self) -> bool: ...


class FutureCallback[T_Ret](Protocol):
    def __call__(self, res: FutureStatus[T_Ret], /) -> object: ...


if TYPE_CHECKING:
    _FS: FutureStatus[None] = cast(asyncio.Future[None], None)
    cast(asyncio.Future[None], None).add_done_callback(cast(FutureCallback[None], None))
