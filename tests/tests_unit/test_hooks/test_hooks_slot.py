from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest

from onetl.hooks import hook, slot, support_hooks

log = logging.getLogger(__name__)


def test_hooks_slot_skip_context(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

    @Calculator.plus.bind
    @hook
    def callback1(self, arg: int):
        return 123

    @Calculator.plus.bind
    @hook
    def callback2(self, arg: int):
        return 234

    with caplog.at_level(logging.INFO):
        # hooks are enabled
        assert Calculator(1).plus(2) == 234

        with Calculator.plus.skip_hooks():
            # hooks are stopped by context manager
            result = Calculator(2).plus(1)
            assert result == 3

        # hooks are still enabled
        assert Calculator(1).plus(3) == 234

        Calculator.plus.suspend_hooks()

        with Calculator.plus.skip_hooks():
            # hooks are disabled
            result = Calculator(2).plus(2)
            assert result == 4

        # skip restores previous state
        result = Calculator(1).plus(4)
        assert result == 5


def test_hooks_slot_skip_decorator(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

    @Calculator.plus.bind
    @hook
    def callback1(self, arg: int):
        return 123

    @Calculator.plus.bind
    @hook
    def callback2(self, arg: int):
        return 234

    def call_with_hooks(data: int, arg: int):
        return Calculator(data).plus(arg)

    @Calculator.plus.skip_hooks()
    def call_without_hooks(data: int, arg: int):
        return Calculator(data).plus(arg)

    with caplog.at_level(logging.INFO):
        # hooks are enabled
        result = call_with_hooks(1, 2)
        assert result == 234

        # hooks are stopped by decorator
        result = call_without_hooks(2, 1)
        assert result == 3

        # hooks are still enabled
        result = call_with_hooks(1, 3)
        assert result == 234

        Calculator.plus.suspend_hooks()

        # hooks are disabled
        result = call_without_hooks(2, 2)
        assert result == 4

        # previous state is restored
        result = call_with_hooks(1, 4)
        assert result == 5


def test_hooks_slot_resume_and_stop(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

    @Calculator.plus.bind
    @hook
    def callback1(self, arg: int):
        return 123

    @Calculator.plus.bind
    @hook
    def callback2(self, arg: int):
        return 234

    @Calculator.plus.bind
    @hook(enabled=False)
    def callback3(self, arg: int):
        # stop & resume does not change hook state
        # they also have higher priority
        raise AssertionError("Never called")

    with caplog.at_level(logging.INFO):
        Calculator.plus.suspend_hooks()

        # hooks are stopped
        assert Calculator(1).plus(2) == 3

        Calculator.plus.resume_hooks()

        # hooks are resumed
        assert Calculator(2).plus(1) == 234

        Calculator.plus.suspend_hooks()

        # hooks are stopped again
        assert Calculator(5).plus(1) == 6


def test_hooks_slot_wrong_method_type():
    error_msg = "@slot decorator could be applied to only to methods of class, got <class 'property'>"
    with pytest.raises(TypeError, match=error_msg):

        @support_hooks
        @dataclass
        class Calculator:
            data: int

            @slot
            @property
            def plus(self):
                return self.data


def test_hooks_slot_private_methods_not_allowed():
    error_msg = "@slot decorator could be applied to public methods only, got '_method'"
    with pytest.raises(ValueError, match=error_msg):

        @support_hooks
        @dataclass
        class Calculator:
            data: int

            @slot
            def _method(self):
                return self.data


def test_hooks_slot_dunders_are_allowed(caplog):
    @support_hooks
    class Calculator:
        @slot
        def __init__(self, data: int):
            self.data = data

    @Calculator.__init__.bind
    @hook
    def callback(self, *args):
        log.info("Executed callback on constructor")

    with caplog.at_level(logging.INFO):
        Calculator(1)

        assert "Executed callback on constructor" in caplog.text


def test_hooks_slot_added_twice():
    error_msg = "Cannot place @slot hook twice on the same method"
    with pytest.raises(SyntaxError, match=error_msg):

        @support_hooks
        @dataclass
        class Calculator:
            data: int

            @slot
            @slot
            def plus(self):
                return self.data


def test_hooks_support_hooks_without_slots():
    error_msg = "@support_hooks can be used only with @slot decorator on some of class methods"
    with pytest.raises(SyntaxError, match=error_msg):

        @support_hooks
        @dataclass
        class Calculator:
            data: int


def test_hooks_methods_without_slot_are_unchanged():
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int):
            return self.data + arg

        def another_method(self, arg: int):
            return self.data * arg

    assert hasattr(Calculator.plus, "suspend_hooks")
    assert not hasattr(Calculator.another_method, "suspend_hooks")


def test_hooks_slot_can_register_only_hook():
    error_msg = r"@.*Calculator\.plus\.bind decorator can be used only on top function marked with @hook"
    with pytest.raises(TypeError, match=error_msg):

        @support_hooks
        class Calculator:
            @slot
            def plus(self, data: int):
                return data

        @Calculator.plus.bind
        def callback(self, *args):
            pass
