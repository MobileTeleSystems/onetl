from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest

from onetl.hooks import HookPriority, hook, slot, support_hooks

log = logging.getLogger(__name__)


def test_hook_disabled():
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

    @Calculator.plus.bind
    @hook(enabled=False)
    def callback(self, arg: int):
        return 123

    # hook is disabled
    assert Calculator(1).plus(2) == 3


def test_hook_priority(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook(priority=HookPriority.NORMAL)
    def intermediate_callback1(self, arg: int):
        log.info("Called intermediate1 callback with %s and %s", self.data, arg)

    @Calculator.plus.bind
    @hook(priority=HookPriority.NORMAL)
    def intermediate_callback2(self, arg: int):
        log.info("Called intermediate2 callback with %s and %s", self.data, arg)

    @Calculator.plus.bind
    @hook(priority=HookPriority.LAST)
    def last_callback1(self, arg: int):
        log.info("Called last1 callback with %s and %s", self.data, arg)

    @Calculator.plus.bind
    @hook(priority=HookPriority.LAST)
    def last_callback2(self, arg: int):
        log.info("Called last2 callback with %s and %s", self.data, arg)

    @Calculator.plus.bind
    @hook(priority=HookPriority.FIRST)
    def first_callback1(self, arg: int):
        log.info("Called first1 callback with %s and %s", self.data, arg)

    @Calculator.plus.bind
    @hook(priority=HookPriority.FIRST)
    def first_callback2(self, arg: int):
        log.info("Called first2 callback with %s and %s", self.data, arg)

    with caplog.at_level(logging.INFO):
        Calculator(1).plus(2)

        first1_call_line = None
        first2_call_line = None
        intermediate1_call_line = None
        intermediate2_call_line = None
        last1_call_line = None
        last2_call_line = None

        for i, record in enumerate(caplog.records):
            if "Called first1 callback with 1 and 2" in record.message:
                first1_call_line = i
            if "Called first2 callback with 1 and 2" in record.message:
                first2_call_line = i
            if "Called intermediate1 callback with 1 and 2" in record.message:
                intermediate1_call_line = i
            if "Called intermediate2 callback with 1 and 2" in record.message:
                intermediate2_call_line = i
            if "Called last1 callback with 1 and 2" in record.message:
                last1_call_line = i
            if "Called last2 callback with 1 and 2" in record.message:
                last2_call_line = i

        # all callbacks are executed
        assert first1_call_line is not None
        assert first2_call_line is not None
        assert intermediate1_call_line is not None
        assert intermediate2_call_line is not None
        assert last1_call_line is not None
        assert last2_call_line is not None

        # but they are executed in order FIRST -> NORMAL -> LAST,
        assert first2_call_line < intermediate1_call_line
        assert intermediate2_call_line < last1_call_line

        # and hooks within the same priority are executed on the same order they were registered
        assert first1_call_line < first2_call_line
        assert intermediate1_call_line < intermediate2_call_line
        assert last1_call_line < last2_call_line


def test_hook_skip_context():
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

    @Calculator.plus.bind
    @hook
    def callback(self, arg: int):
        return 123

    # hook is enabled
    assert Calculator(1).plus(2) == 123

    # hook is disabled by decorator
    assert callback.enabled

    with callback.skip():
        assert not callback.enabled
        assert Calculator(2).plus(1) == 3

    # state restored successfully
    assert callback.enabled


def test_hook_skip_decorator():
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

    @Calculator.plus.bind
    @hook
    def callback(self, arg: int):
        return 123

    def call_with_hook(data: int, arg: int):
        return Calculator(data).plus(arg)

    @callback.skip()
    def call_without_hook(data: int, arg: int):
        return Calculator(data).plus(arg)

    # hook is enabled
    assert call_with_hook(1, 2) == 123

    # hook is disabled by decorator
    assert callback.enabled
    assert call_without_hook(2, 1) == 3
    # state restored successfully
    assert callback.enabled


def test_hook_enable_and_disable():
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

    @Calculator.plus.bind
    @hook(enabled=False)
    def callback(self, arg: int):
        return 123

    assert not callback.enabled
    assert Calculator(1).plus(1) == 2

    callback.enable()

    assert callback.enabled
    assert Calculator(2).plus(1) == 123

    callback.disable()

    assert not callback.enabled
    assert Calculator(1).plus(2) == 3


def test_hook_enable_and_disable_multiple():
    @support_hooks
    @dataclass
    class Calculator1:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

    @support_hooks
    @dataclass
    class Calculator2:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data * arg

    @Calculator1.plus.bind
    @Calculator2.plus.bind
    @hook(enabled=False)
    def callback(self, arg: int):
        return 123

    assert not callback.enabled
    assert Calculator1(1).plus(1) == 2
    assert Calculator2(1).plus(1) == 1

    callback.enable()

    assert callback.enabled
    assert Calculator1(2).plus(1) == 123
    assert Calculator2(2).plus(1) == 123

    callback.disable()
    assert not callback.enabled
    assert Calculator1(1).plus(2) == 3
    assert Calculator2(1).plus(2) == 2


def test_hook_applied_twice():
    with pytest.raises(TypeError, match="@hook decorator can be applied only once"):

        @support_hooks
        @dataclass
        class Calculator:
            data: int

            @slot
            def plus(self, arg: int) -> int:
                return self.data + arg

        @hook(enabled=True)
        @Calculator.plus.bind
        @hook(enabled=False)
        def callback(self, arg: int):
            return 123
