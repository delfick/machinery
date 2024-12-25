import dataclasses

import pytest

from machinery import helpers as hp


class TestMemoizedProperty:
    def test_it_memoizes(self) -> None:
        called: list[int] = []

        class Thing:
            _memoized_cache: dict[str, object]

            def __init__(self):
                self._memoized_cache = {}

            @hp.memoized_property
            def blah(self) -> str:
                called.append(1)
                return "stuff"

        thing = Thing()
        assert thing.blah == "stuff"
        assert called == [1]
        assert thing.blah == "stuff"
        assert called == [1]
        assert thing.blah == "stuff"
        assert called == [1]

    def test_it_works_on_an_dataclasses_class(self) -> None:
        called: list[int] = []

        @dataclasses.dataclass
        class Thing:
            _memoized_cache: dict[str, object] = dataclasses.field(
                init=False, default_factory=dict
            )

            @hp.memoized_property
            def blah(self) -> str:
                called.append(1)
                return "stuff"

        thing = Thing()
        assert thing.blah == "stuff"
        assert called == [1]
        assert thing.blah == "stuff"
        assert called == [1]
        assert thing.blah == "stuff"
        assert called == [1]

    def test_it_does_not_allow_setting_the_value(self) -> None:
        called: list[int] = []

        class Thing:
            _memoized_cache: dict[str, object]

            def __init__(self):
                self._memoized_cache = {}

            @hp.memoized_property
            def blah(self) -> str:
                called.append(1)
                return "stuff"

        thing = Thing()
        with pytest.raises(AttributeError):
            thing.blah = "other"

    def test_it_allows_deleting_the_value(self) -> None:
        called: list[int] = []

        class Thing:
            _memoized_cache: dict[str, object]

            def __init__(self):
                self._memoized_cache = {}

            @hp.memoized_property
            def blah(self) -> str:
                called.append(1)
                return "stuff"

        thing = Thing()
        assert thing.blah == "stuff"
        assert called == [1]
        del thing.blah
        assert thing.blah == "stuff"
        assert called == [1, 1]
        assert thing.blah == "stuff"
        assert called == [1, 1]

    def test_it_keeps_the_type_annotation(self) -> None:
        class Thing:
            _memoized_cache: dict[str, object]

            def __init__(self):
                self._memoized_cache = {}

            @hp.memoized_property
            def blah(self) -> str:
                return "stuff"

        def a(things: str) -> None:
            pass

        # will make mypy complain if it's broken
        a(Thing().blah)
