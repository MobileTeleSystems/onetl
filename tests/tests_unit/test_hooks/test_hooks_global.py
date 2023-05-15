from __future__ import annotations

from dataclasses import dataclass

from onetl.hooks import (
    hook,
    resume_all_hooks,
    skip_all_hooks,
    slot,
    stop_all_hooks,
    support_hooks,
)


def test_hooks_global_stop_and_resume(request):
    request.addfinalizer(resume_all_hooks)

    @support_hooks
    @dataclass
    class Calculator1:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

        @slot
        def multiply(self, arg: int) -> int:
            return self.data * arg

    @support_hooks
    @dataclass
    class Calculator2:
        data: int

        @slot
        def power(self, arg: int) -> int:
            return self.data**arg

    @Calculator1.plus.bind
    @hook
    def callback1(self, arg: int):
        return 123

    @Calculator1.plus.bind
    @hook
    def callback2(self, arg: int):
        return 234

    @Calculator1.multiply.bind
    @hook
    def another_callback(self, arg: int):
        return 345

    @Calculator2.power.bind
    @hook
    def more_callback(self, arg: int):
        return 567

    @Calculator1.plus.bind
    @Calculator2.power.bind
    @hook(enabled=False)
    def never_called(self, arg: int):
        # stop & resume does not affect hook state, it should be enabled explicitly
        raise AssertionError("Never called")

    assert Calculator1(1).plus(1) == 234
    assert Calculator1(1).multiply(1) == 345
    assert Calculator2(1).power(1) == 567

    stop_all_hooks()
    assert Calculator1(2).plus(1) == 3
    assert Calculator1(2).multiply(1) == 2
    assert Calculator2(2).power(1) == 2

    # this have no affect on global state
    Calculator1.resume_hooks()

    assert Calculator1(2).plus(2) == 4
    assert Calculator1(2).multiply(2) == 4
    assert Calculator2(2).power(2) == 4

    # global state influences all new classes and new hooks
    @support_hooks
    @dataclass
    class Calculator3:
        data: int

        @slot
        def modulus(self, arg: int) -> int:
            return self.data % arg

    @Calculator3.modulus.bind
    @hook
    def callback4(self, arg: int):
        return 789

    assert Calculator3(5).modulus(3) == 2

    resume_all_hooks()
    assert Calculator1(1).plus(3) == 234
    assert Calculator1(1).multiply(3) == 345
    assert Calculator2(1).power(3) == 567
    assert Calculator3(5).modulus(4) == 789

    stop_all_hooks()
    assert Calculator1(2).plus(3) == 5
    assert Calculator1(2).multiply(3) == 6
    assert Calculator2(2).power(3) == 8
    assert Calculator3(5).modulus(2) == 1


def test_hooks_global_skip_context(request):
    request.addfinalizer(resume_all_hooks)

    @support_hooks
    @dataclass
    class Calculator1:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

        @slot
        def multiply(self, arg: int) -> int:
            return self.data * arg

    @support_hooks
    @dataclass
    class Calculator2:
        data: int

        @slot
        def power(self, arg: int) -> int:
            return self.data**arg

    @Calculator1.plus.bind
    @hook
    def callback1(self, arg: int):
        return 123

    @Calculator1.plus.bind
    @hook
    def callback2(self, arg: int):
        return 234

    @Calculator1.multiply.bind
    @hook
    def another_callback(self, arg: int):
        return 345

    @Calculator2.power.bind
    @hook
    def more_callback(self, arg: int):
        return 567

    @Calculator1.plus.bind
    @Calculator2.power.bind
    @hook(enabled=False)
    def never_called(self, arg: int):
        # skip does not affect hook state, it should be enabled explicitly
        raise AssertionError("Never called")

    assert Calculator1(1).plus(1) == 234
    assert Calculator1(1).multiply(1) == 345
    assert Calculator2(1).power(1) == 567

    with skip_all_hooks():
        # this have no affect on global state
        Calculator1.resume_hooks()

        assert Calculator1(2).plus(1) == 3
        assert Calculator1(2).multiply(1) == 2
        assert Calculator2(2).power(1) == 2

        # global state influences all new classes and new hooks
        @support_hooks
        @dataclass
        class Calculator3:
            data: int

            @slot
            def modulus(self, arg: int) -> int:
                return self.data % arg

        @Calculator3.modulus.bind
        @hook
        def callback4(self, arg: int):
            return 789

        assert Calculator3(5).modulus(3) == 2

    # hooks are enabled
    assert Calculator1(1).plus(2) == 234
    assert Calculator1(1).multiply(2) == 345
    assert Calculator2(1).power(2) == 567
    assert Calculator3(5).modulus(2) == 789

    stop_all_hooks()

    with skip_all_hooks():
        # hooks are already disabled
        assert Calculator1(2).plus(2) == 4
        assert Calculator1(2).multiply(2) == 4
        assert Calculator2(2).power(2) == 4
        assert Calculator3(5).modulus(4) == 1

    # skip restores previous state
    assert Calculator1(1).plus(3) == 4
    assert Calculator1(1).multiply(3) == 3
    assert Calculator2(1).power(3) == 1
    assert Calculator3(5).modulus(1) == 0


def test_hooks_global_skip_decorator(request):
    request.addfinalizer(resume_all_hooks)

    @support_hooks
    @dataclass
    class Calculator1:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

        @slot
        def multiply(self, arg: int) -> int:
            return self.data * arg

    @support_hooks
    @dataclass
    class Calculator2:
        data: int

        @slot
        def power(self, arg: int) -> int:
            return self.data**arg

    @Calculator1.plus.bind
    @hook
    def callback1(self, arg: int):
        return 123

    @Calculator1.plus.bind
    @hook
    def callback2(self, arg: int):
        return 234

    @Calculator1.multiply.bind
    @hook
    def another_callback(self, arg: int):
        return 345

    @Calculator2.power.bind
    @hook
    def more_callback(self, arg: int):
        return 567

    @Calculator1.plus.bind
    @Calculator2.power.bind
    @hook(enabled=False)
    def never_called(self, arg: int):
        # skip does not affect hook state, it should be enabled explicitly
        raise AssertionError("Never called")

    assert Calculator1(1).plus(1) == 234
    assert Calculator1(1).multiply(1) == 345
    assert Calculator2(1).power(1) == 567

    @skip_all_hooks()
    def check_skipped():
        # this have no affect on global state
        Calculator1.resume_hooks()

        assert Calculator1(2).plus(1) == 3
        assert Calculator1(2).multiply(1) == 2
        assert Calculator2(2).power(1) == 2

        # global state influences all new classes and new hooks
        @support_hooks
        @dataclass
        class Calculator3:
            data: int

            @slot
            def modulus(self, arg: int) -> int:
                return self.data % arg

        @Calculator3.modulus.bind
        @hook
        def callback4(self, arg: int):
            return 789

        assert Calculator3(5).modulus(3) == 2

    check_skipped()

    assert Calculator1(1).plus(2) == 234
    assert Calculator1(1).multiply(2) == 345
    assert Calculator2(1).power(2) == 567

    stop_all_hooks()

    # hooks are already disabled
    check_skipped()

    # skip restores previous state
    assert Calculator1(1).plus(3) == 4
    assert Calculator1(1).multiply(3) == 3
    assert Calculator2(1).power(3) == 1
