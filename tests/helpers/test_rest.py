from machinery import helpers as hp


class TestFutureToString:
    def test_just_reprs_a_not_future(self) -> None:
        class Thing:
            def __repr__(s):
                return "<REPR THING>"

        assert hp.fut_to_string(Thing()) == "<REPR THING>"

    def test_says_if_the_future_is_pending(self) -> None:
        fut = hp.create_future(name="one")
        assert hp.fut_to_string(fut) == "<Future#one(pending)>"

        fut = hp.create_future()
        assert hp.fut_to_string(fut) == "<Future#None(pending)>"

    def test_says_if_the_future_is_cancelled(self) -> None:
        fut = hp.create_future(name="one")
        fut.cancel()
        assert hp.fut_to_string(fut) == "<Future#one(cancelled)>"

        fut = hp.create_future()
        fut.cancel()
        assert hp.fut_to_string(fut) == "<Future#None(cancelled)>"

    def test_says_if_the_future_has_an_exception(self) -> None:
        fut = hp.create_future(name="one")
        fut.set_exception(ValueError("HI"))
        assert hp.fut_to_string(fut) == "<Future#one(exception:ValueError:HI)>"

        fut = hp.create_future()
        fut.set_exception(TypeError("NOPE"))
        assert hp.fut_to_string(fut) == "<Future#None(exception:TypeError:NOPE)>"

    def test_says_if_the_future_has_a_result(self) -> None:
        fut = hp.create_future(name="one")
        fut.set_result(True)
        assert hp.fut_to_string(fut) == "<Future#one(result)>"

        fut = hp.create_future()
        fut.set_result(False)
        assert hp.fut_to_string(fut) == "<Future#None(result)>"
