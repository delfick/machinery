from __future__ import annotations

from collections.abc import Callable, MutableMapping
from typing import Generic, TypeVar, cast, overload

T_Ret = TypeVar("T_Ret")


class memoized_property(Generic[T_Ret]):
    """
    A descriptor that memoizes the value it creates. This requires that
    the object the descriptor is on has a ``_memoized_cache`` attribute that is
    a mutable mapping that lets us save the generated values.

    Usage is::

        class MyClass:
            def __init__(self):
                self._memoized_cache = {}

            @memoized_property
            def expensive(self)->int:
                return perform_expensive_operation()


        instance = MyClass()
        assert instance.expensive == 20
        assert instance.expensive == 20 # This time expensive operation is not run

        # It's possible to remove the cached value
        del instance.expensive
        assert instance.expensive == 20 # Expensive operation runs again
    """

    name: str

    class Empty:
        pass

    def __init__(self, func: Callable[..., T_Ret]) -> None:
        self.func = func
        self.__doc__ = func.__doc__

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name
        if "_memoized_cache" not in owner.__annotations__:
            raise NotImplementedError(
                "The class this is attached to needs a _memoized_cache on it"
            )

    def cache(self, instance: object) -> MutableMapping[str, object]:
        cache = getattr(instance, "_memoized_cache", None)
        assert isinstance(cache, MutableMapping)
        return cache

    @overload
    def __get__(self, instance: None, owner: None) -> memoized_property: ...

    @overload
    def __get__(self, instance: object, owner: type[object]) -> T_Ret: ...

    def __get__(
        self, instance: object | None, owner: type[object] | None = None
    ) -> memoized_property | T_Ret:
        if instance is None:
            return self

        cache = self.cache(instance)

        if self.name not in cache:
            cache[self.name] = self.func(instance)

        return cast(T_Ret, cache[self.name])

    def __delete__(self, instance: object) -> None:
        cache = self.cache(instance)
        if self.name in cache:
            del cache[self.name]
