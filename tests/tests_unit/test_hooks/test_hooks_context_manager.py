from __future__ import annotations

import logging
import textwrap
from dataclasses import dataclass

import pytest

from onetl.exception import SignatureError
from onetl.hooks import hook, slot, support_hooks

log = logging.getLogger(__name__)


def test_hooks_execute_context_manager_enter(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class BeforeCallback:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            log.info("Called before callback with %s and %s", self.instance.data, self.arg)
            return self

        def __exit__(self, *args):
            return False

    with caplog.at_level(logging.INFO):
        # result was not changed
        assert Calculator(1).plus(2) == 3

        before_call_line = None
        method_call_line = None

        for i, record in enumerate(caplog.records):
            if "Called before callback with 1 and 2" in record.message:
                before_call_line = i
            if "Called original method with 1 and 2" in record.message:
                method_call_line = i

        # both callback and original method were executed
        assert before_call_line is not None
        assert method_call_line is not None

        # method is called after callback
        assert before_call_line < method_call_line


def test_hooks_execute_context_manager_exit(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class AfterCallback:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            return self

        def __exit__(self, *args):
            log.info("Called after callback with %s and %s", self.instance.data, self.arg)
            return False

    with caplog.at_level(logging.INFO):
        # result was not changed
        assert Calculator(1).plus(2) == 3

        after_call_line = None
        method_call_line = None

        for i, record in enumerate(caplog.records):
            if "Called after callback with 1 and 2" in record.message:
                after_call_line = i
            if "Called original method with 1 and 2" in record.message:
                method_call_line = i

        # both callback and original method were executed
        assert after_call_line is not None
        assert method_call_line is not None

        # method is called before callback
        assert after_call_line > method_call_line


def test_hooks_execute_context_manager_process_result_replace(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class ReplaceCallback:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def process_result(self, result: int) -> int:
            log.info("Called replace callback with %s and %s", self.instance.data, self.arg)
            return 10

    with caplog.at_level(logging.INFO):
        # result was replaced successfully
        assert Calculator(1).plus(2) == 10

        replace_call_line = None
        method_call_line = None

        for i, record in enumerate(caplog.records):
            if "Called replace callback with 1 and 2" in record.message:
                replace_call_line = i
            if "Called original method with 1 and 2" in record.message:
                method_call_line = i

        # both callback and original method were executed
        assert replace_call_line is not None
        assert method_call_line is not None

        # replace callback is executed after the method call
        assert method_call_line < replace_call_line


def test_hooks_execute_context_manager_process_result_last_wins(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class ReplaceCallback2:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def process_result(self, result: int) -> int:
            return 123

    @Calculator.plus.bind
    @hook
    class ReplaceCallback1:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def process_result(self, result: int) -> int:
            return 234

    # the last hook result is used
    assert Calculator(1).plus(2) == 234


def test_hooks_execute_context_manager_process_result_modify(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class ModifyCallback:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def process_result(self, result: int) -> int:
            log.info("Called modify callback with %s and %s", self.instance.data, self.arg)
            return result + 10

    with caplog.at_level(logging.INFO):
        # result was modified successfully
        assert Calculator(1).plus(2) == 13

        modify_call_line = None
        method_call_line = None

        for i, record in enumerate(caplog.records):
            if "Called modify callback with 1 and 2" in record.message:
                modify_call_line = i
            if "Called original method with 1 and 2" in record.message:
                method_call_line = i

        # both callback and original method were executed
        assert modify_call_line is not None
        assert method_call_line is not None

        # method is called before modify callback
        assert method_call_line < modify_call_line


def test_hooks_execute_context_manager_catch_exception(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            raise TypeError(f"Raised with {self.data} and {arg}")

    @Calculator.plus.bind
    @hook
    class ContextCallback:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            log.info("Before method call")
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            if exc_type:
                log.exception("Context caught exception", exc_info=(exc_type, exc_value, traceback))
                del traceback
                raise RuntimeError("Replaced") from exc_value

            log.info("After method call")

    # exception successfully caught
    with pytest.raises(RuntimeError, match="Replaced"), caplog.at_level(logging.INFO):
        Calculator(1).plus(2)

    before_call_line = None
    after_call_line = None
    caught_exception_line = None
    method_call_line = None

    for i, record in enumerate(caplog.records):
        if "Before method call" in record.message:
            before_call_line = i
        if "After method call" in record.message:
            after_call_line = i
        if "Context caught exception" in record.message:
            caught_exception_line = i
        if "Called original method with 1 and 2" in record.message:
            method_call_line = i

    # both callback and original method were executed
    assert before_call_line is not None
    assert method_call_line is not None
    assert caught_exception_line is not None
    # code after yield is not executed because of exception propagation mechanism
    assert after_call_line is None

    # method is called before modify callback
    assert before_call_line < method_call_line < caught_exception_line


def test_hooks_execute_context_manager_pass_method_name(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class BeforeCallback:
        def __init__(self, instance: Calculator, arg: int, method_name: str):
            self.instance = instance
            self.arg = arg
            self.plus_name = method_name

        def __enter__(self):
            log.info("Called before callback with %s and %s, by '%s'", self.instance.data, self.arg, self.plus_name)
            return self

        def __exit__(self, *args):
            return False

    with caplog.at_level(logging.INFO):
        Calculator(1).plus(2)

        assert "Called before callback with 1 and 2, by 'plus'" in caplog.text


def test_hooks_execute_context_manager_different_method_types(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

        @slot
        @classmethod
        def class_method(cls) -> int:
            log.info("Called original class method")
            return 123

        @slot
        @staticmethod
        def static_method(arg: int, arg2: str) -> int:
            log.info("Called original static method with %s and %s", arg, arg2)
            return arg * arg2

    @Calculator.plus.bind
    @hook
    class MethodCallback:
        def __init__(self, instance: Calculator, arg: int):
            assert isinstance(instance, Calculator)
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            log.info("Called method callback with %s and %s", self.instance.data, self.arg)
            return self

        def __exit__(self, *args):
            return False

    @Calculator.class_method.bind
    @hook
    class ClassMethodCallback:
        def __init__(self, klass: type):
            assert klass is Calculator
            self.klass = klass

        def __enter__(self):
            log.info("Called class method callback")
            return self

        def __exit__(self, *args):
            return False

    @Calculator.static_method.bind
    @hook
    class StaticMethodCallback:
        def __init__(self, arg: int, arg2: str):
            self.arg = arg
            self.arg2 = arg2

        def __enter__(self):
            log.info("Called static method callback with %s and %s", self.arg, self.arg2)
            return self

        def __exit__(self, *args):
            return False

    with caplog.at_level(logging.INFO):
        method_result = Calculator(1).plus(2)
        assert "Called original method with 1 and 2" in caplog.text
        assert "Called method callback with 1 and 2" in caplog.text
        assert method_result == 3

        class_method_result = Calculator.class_method()
        assert "Called original class method" in caplog.text
        assert "Called class method callback" in caplog.text
        assert class_method_result == 123

        static_method_result = Calculator.static_method(1, 2)
        assert "Called original static method with 1 and 2" in caplog.text
        assert "Called static method callback with 1 and 2" in caplog.text
        assert static_method_result == 2


def test_hooks_execute_context_manager_init_is_raising_exception(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class BeforeCallback:
        def __init__(self, instance: Calculator, arg: int):
            if arg == 3:
                raise ValueError("Argument value 3 is not allowed")
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    # exception successfully raised
    with pytest.raises(ValueError, match="Argument value 3 is not allowed"), caplog.at_level(logging.INFO):
        Calculator(1).plus(3)

    assert "|Hooks| Error while executing a hook" in caplog.text

    # exception is raised before calling the original method
    assert "Called original method with" not in caplog.text

    # if is working as expected
    assert Calculator(1).plus(2) == 3


def test_hooks_execute_context_manager_enter_is_raising_exception(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class BeforeCallback:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            if self.arg == 3:
                raise ValueError("Argument value 3 is not allowed")
            return self

        def __exit__(self, *args):
            return False

    # exception successfully raised
    with pytest.raises(ValueError, match="Argument value 3 is not allowed"), caplog.at_level(logging.INFO):
        Calculator(1).plus(3)

    # this error message is logged only for before hooks, because we cannot distinguish
    # internal method errors from ones raised by a hook. only stacktrace can help us to debug this
    assert "|Hooks| Error while executing a callback" not in caplog.text

    # exception is raised before calling the original method
    assert "Called original method with" not in caplog.text

    # if is working as expected
    assert Calculator(1).plus(2) == 3


def test_hooks_execute_context_manager_exit_is_raising_exception(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class BeforeCallback:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def process_result(self, result: int) -> int:
            if result == 4:
                raise ValueError("Result value 4 is not allowed")
            return result

    # exception successfully raised
    with pytest.raises(ValueError, match="Result value 4 is not allowed"), caplog.at_level(logging.INFO):
        Calculator(1).plus(3)

    # this error message is logged only for before hooks, because we cannot distinguish
    # internal method errors from ones raised by a hook. only stacktrace can help us to debug this
    assert "|Hooks| Error while executing a callback" not in caplog.text

    # exception is raised after calling the original method
    assert "Called original method with" in caplog.text

    # if is working as expected
    assert Calculator(1).plus(2) == 3


def test_hooks_execute_context_manager_process_result_is_raising_exception(caplog):
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            log.info("Called original method with %s and %s", self.data, arg)
            return self.data + arg

    @Calculator.plus.bind
    @hook
    class BeforeCallback:
        def __init__(self, instance: Calculator, arg: int):
            self.instance = instance
            self.arg = arg

        def __enter__(self):
            return self

        def __exit__(self, *args):
            if self.arg == 3:
                raise ValueError("Argument value 3 is not allowed")
            return False

    # exception successfully raised
    with pytest.raises(ValueError, match="Argument value 3 is not allowed"), caplog.at_level(logging.INFO):
        Calculator(1).plus(3)

    # this error message is logged only for before hooks, because we cannot distinguish
    # internal method errors from ones raised by a hook. only stacktrace can help us to debug this
    assert "|Hooks| Error while executing a callback" not in caplog.text

    # exception is raised after calling the original method
    assert "Called original method with" in caplog.text

    # if is working as expected
    assert Calculator(1).plus(2) == 3


def test_hooks_execute_context_manager_wrong_signature():
    @support_hooks
    @dataclass
    class Calculator:
        data: int

        @slot
        def plus(self, arg: int) -> int:
            pass

    @Calculator.plus.bind
    @hook
    class MissingArg:
        def __init__(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    local_name = "test_hooks_context_manager.test_hooks_execute_context_manager_wrong_signature"
    method_name = f"{local_name}.<locals>.Calculator.plus"
    hook_name = f"{local_name}.<locals>.MissingArg"

    error_msg = textwrap.dedent(
        rf"""
        Error while passing method arguments to a hook.

        Method name: '{method_name}'
        Method source: '{__file__}:\d+'
        Method signature:
            \(self, arg: 'int'\) -> 'int'

        Hook name: '{hook_name}'
        Hook source: '{__file__}:\d+'
        Hook signature:
            \(\)
        """,
    )

    with pytest.raises(SignatureError, match=error_msg):
        Calculator(1).plus(3)
