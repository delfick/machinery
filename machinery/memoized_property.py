import typing as tp

T = tp.TypeVar("T")
R = tp.TypeVar("R")


class MISSING:
    pass


class _optional_property_depends_on(tp.Generic[T, R]):
    def __init__(
        self,
        *,
        dependency_type: type,
        value_type: type,
        func: tp.Callable[[tp.Any, T], R],
        dependency_getter: tp.Callable[[tp.Any, R], T],
        dependency: str,
    ):
        self.func = func
        self.dependency = dependency
        self.dependency_getter = dependency_getter

        self.value_type = value_type
        self.dependency_type = dependency_type

    def __set_name__(self, owner: type, name: str):
        self.name = f"_{name}"
        self.last_dep_name = f"_dep_{name}"

    @tp.overload
    def __get__(self, obj: None, objtype: None) -> "_optional_property_depends_on":
        ...

    @tp.overload
    def __get__(self, obj: object, objtype: type[object]) -> R | None:
        ...

    def __get__(
        self, obj: object | None, objtype: type[object] | None = None
    ) -> tp.Union["_optional_property_depends_on", R | None]:
        if obj is None:
            return self

        current_dependency: object = MISSING
        now_dependency = getattr(obj, self.dependency, MISSING)

        current = getattr(obj, self.name, MISSING)
        if current not in (MISSING, None):
            current_dependency = self.dependency_getter(obj, tp.cast(R, current))

        remove_existing = False
        if now_dependency is MISSING:
            remove_existing = True

        elif current_dependency != now_dependency:
            remove_existing = True

        if remove_existing and hasattr(obj, self.name):
            delattr(obj, self.name)

        if now_dependency in (MISSING, None):
            return None

        current = getattr(obj, self.name, MISSING)
        if current is not MISSING:
            return tp.cast(R, current)

        if now_dependency is not None and not isinstance(now_dependency, self.dependency_type):
            raise ValueError(
                f"Expected dependency information to be a {self.dependency_type}, got {type(now_dependency)}"
            )

        new_value = self.func(obj, tp.cast(T, now_dependency))
        self.__set__(obj, new_value)
        return new_value

    def __set__(self, obj: object, value: tp.Any) -> None:
        if value is None:
            if hasattr(obj, self.name):
                delattr(obj, self.name)
            return None

        if not isinstance(value, self.value_type):
            raise ValueError(
                f"Can only set {self.name[1:]} to {self.value_type} instances, not {type(value)}"
            )

        setattr(obj, self.dependency, self.dependency_getter(obj, tp.cast(R, value)))
        setattr(obj, self.name, value)

    def __delete__(self, obj: object) -> None:
        if hasattr(obj, self.name):
            delattr(obj, self.name)


class _always_property_depends_on(tp.Generic[T, R]):
    def __init__(
        self,
        *,
        dependency_type: type,
        value_type: type,
        func: tp.Callable[[tp.Any, T], R],
        dependency_getter: tp.Callable[[tp.Any, R], T],
        dependency: str,
    ):
        self.func = func
        self.dependency = dependency
        self.dependency_getter = dependency_getter

        self.value_type = value_type
        self.dependency_type = dependency_type

    def __set_name__(self, owner: type, name: str):
        self.name = f"_{name}"
        self.last_dep_name = f"_dep_{name}"

    @tp.overload
    def __get__(self, obj: None, objtype: None) -> "_always_property_depends_on":
        ...

    @tp.overload
    def __get__(self, obj: object, objtype: type[object]) -> R:
        ...

    def __get__(
        self, obj: object | None, objtype: type[object] | None = None
    ) -> tp.Union["_always_property_depends_on", R]:
        if obj is None:
            return self

        current_dependency: object = MISSING
        now_dependency = getattr(obj, self.dependency, MISSING)

        current = getattr(obj, self.name, MISSING)
        if current is None:
            raise ValueError(f"Don't expect {self.name[1:]} to be None")

        if current is MISSING:
            current_dependency = self.dependency_getter(obj, tp.cast(R, current))

        remove_existing = False
        if now_dependency is MISSING:
            remove_existing = True

        elif current_dependency != now_dependency:
            remove_existing = True

        if remove_existing and hasattr(obj, self.name):
            delattr(obj, self.name)

        if now_dependency is MISSING:
            raise ValueError(f"Expect {self.dependency} to not be None")

        current = getattr(obj, self.name, MISSING)
        if current is not MISSING:
            return tp.cast(R, current)

        if now_dependency is not None and not isinstance(now_dependency, self.dependency_type):
            raise ValueError(
                f"Expected dependency information to be a {self.dependency_type}, got {type(now_dependency)}"
            )

        new_value = self.func(obj, tp.cast(T, now_dependency))
        self.__set__(obj, new_value)
        return new_value

    def __set__(self, obj: object, value: tp.Any) -> None:
        if value is None:
            raise ValueError(f"Cannot have no value for {self.name}")

        if not isinstance(value, self.value_type):
            raise ValueError(
                f"Can only set {self.name[1:]} to {self.value_type} instances, not {type(value)}"
            )

        setattr(obj, self.dependency, self.dependency_getter(obj, tp.cast(R, value)))
        setattr(obj, self.name, value)


def optional_property_depends_on(
    dependency_type: type[T],
    dependency: str,
    value_type: type[R],
    dependency_getter: tp.Callable[[tp.Any, R], T],
) -> tp.Callable[[tp.Callable[[tp.Any, T], R]], _optional_property_depends_on[T, R]]:
    def decorator(func: tp.Callable[[tp.Any, T], R]) -> _optional_property_depends_on[T, R]:
        return _optional_property_depends_on[T, R](
            dependency_type=dependency_type,
            value_type=value_type,
            func=func,
            dependency_getter=dependency_getter,
            dependency=dependency,
        )

    return decorator


def always_property_depends_on(
    dependency_type: type[T],
    dependency: str,
    value_type: type[R],
    dependency_getter: tp.Callable[[tp.Any, R], T],
) -> tp.Callable[[tp.Callable[[tp.Any, T], R]], _always_property_depends_on[T, R]]:
    def decorator(func: tp.Callable[[tp.Any, T], R]) -> _always_property_depends_on[T, R]:
        return _always_property_depends_on[T, R](
            dependency_type=dependency_type,
            value_type=value_type,
            func=func,
            dependency_getter=dependency_getter,
            dependency=dependency,
        )

    return decorator


class Thing:
    attr: str


class ThingActionConfig:
    thing: str

    def _get_thing(self, new_thing: Thing) -> str:
        return new_thing.attr

    @always_property_depends_on(str, "thing", Thing, _get_thing)
    def meter_point(self, thing: str) -> Thing:
        return Thing(thing=thing)
