from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass

from onetl.hooks import hook, slot, support_hooks

log = logging.getLogger(__name__)


def test_hooks_inheritance(caplog):
    @support_hooks
    @dataclass
    class BaseCalc:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called base class method with %s and %s", self.data, arg)
            return self.data + arg

    @support_hooks
    @dataclass
    class NestedCalc(BaseCalc):
        @slot
        def plus(self, arg: int) -> int:
            log.info("Called nested class method with %s and %s", self.data, arg)
            return super().plus(arg) + 10

    base_hook_calls: dict[tuple(int, int), int] = defaultdict(int)

    @BaseCalc.plus.bind
    @hook
    def base_callback(self, arg: int):
        log.info("Called base class callback with %s and %s", self.data, arg)

        base_hook_calls[(self.data, arg)] += 1
        assert base_hook_calls[(self.data, arg)] < 2, "Base callback should not be called twice"

    @NestedCalc.plus.bind
    @hook
    def nested_callback(self, arg: int):
        log.info("Called nested class callback with %s and %s", self.data, arg)

    with caplog.at_level(logging.INFO):
        # only base_callback is executed here
        base_result = BaseCalc(1).plus(2)
        assert base_result == 3

        # both nested_callback and base_callback are executed here
        nested_result = NestedCalc(2).plus(1)
        assert nested_result == 13

    # only base hook is called for base class
    assert "Called base class callback with 1 and 2" in caplog.text
    assert "Called nested class callback with 1 and 2" not in caplog.text

    base_callback_call_line = 0
    nested_callback_call_line = 0

    for i, record in enumerate(caplog.records):
        if "Called base class callback with 2 and 1" in record.message:
            base_callback_call_line = i
        if "Called nested class callback with 2 and 1" in record.message:
            nested_callback_call_line = i

    # both hooks are called for the nested class
    assert base_callback_call_line
    assert nested_callback_call_line

    # base hook was executed before nested one
    assert base_callback_call_line < nested_callback_call_line


def test_hooks_class_stop_and_resume():
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

    Calculator1.suspend_hooks()
    assert Calculator1(2).plus(1) == 3
    assert Calculator1(2).multiply(1) == 2
    assert Calculator2(2).power(1) == 567

    Calculator1.resume_hooks()
    assert Calculator1(1).plus(3) == 234
    assert Calculator1(1).multiply(3) == 345
    assert Calculator2(1).power(3) == 567


def test_hooks_class_skip_context():
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

    with Calculator1.skip_hooks():
        assert Calculator1(2).plus(1) == 3
        assert Calculator1(2).multiply(1) == 2
        assert Calculator2(2).power(1) == 567

    with Calculator2.skip_hooks():
        assert Calculator1(3).plus(1) == 234
        assert Calculator1(3).multiply(1) == 345
        assert Calculator2(3).power(1) == 3

    with Calculator1.skip_hooks(), Calculator2.skip_hooks():
        assert Calculator1(3).plus(3) == 6
        assert Calculator1(3).multiply(3) == 9
        assert Calculator2(3).power(3) == 27


def test_hooks_class_skip_decorator():
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

    @Calculator1.skip_hooks()
    def class1_without_hooks():
        assert Calculator1(2).plus(1) == 3
        assert Calculator1(2).multiply(1) == 2
        assert Calculator2(2).power(1) == 567

    @Calculator2.skip_hooks()
    def class2_without_hooks():
        assert Calculator1(3).plus(1) == 234
        assert Calculator1(3).multiply(1) == 345
        assert Calculator2(3).power(1) == 3

    @Calculator1.skip_hooks()
    @Calculator2.skip_hooks()
    def multiple():
        assert Calculator1(3).plus(3) == 6
        assert Calculator1(3).multiply(3) == 9
        assert Calculator2(3).power(3) == 27

    class1_without_hooks()
    class2_without_hooks()
    multiple()

    assert Calculator1(1).plus(2) == 234
    assert Calculator1(1).multiply(2) == 345
    assert Calculator2(1).power(2) == 567
