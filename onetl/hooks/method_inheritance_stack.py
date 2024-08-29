# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import defaultdict


class MethodInheritanceStack:
    """
    Context manager which can track the method calls within class inheritance diagram.

    For inherited class, overridden method contains a ``super().method(...)`` call.
    If base class method is registered as a slot, and there are some hooks bound to it,
    calling the inherited class method can cause calling the same hook twice -
    both on inherited class and base (due to calling ``super()``).

    This context manager allows to track such ``super()`` calls, allowing to
    prevent the same hook from being executed twice.

    Examples
    --------

    >>> from onetl.hooks import support_hooks, slot
    >>> from onetl.hooks.method_inheritance_stack import MethodInheritanceStack
    >>> @support_hooks
    ... class BaseClass:
    ...     @slot
    ...     def some_method(self, *args, **kwargs):
    ...         pass
    >>> @support_hooks
    ... class MyClass(BaseClass):
    ...    @slot
    ...    def some_method(self, *args, **kwargs):
    ...        self.do_something()
    ...        super().some_method(*args, **kwargs)

    >>> # caused by MyClass.some_method() call
    >>> with MethodInheritanceStack(MyClass, "some_method") as method_call_stack:
    ...     print("MyClass", method_call_stack.level)
    ...     # MyClass.some_method() called super().some_method()
    ...     with MethodInheritanceStack(BaseClass, "some_method") as method_call_stack:
    ...         print("BaseClass", method_call_stack.level)
    MyClass 0
    BaseClass 1
    """

    _stack: dict[type, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def __init__(self, klass: type, method_name: str):
        self.klass = klass
        self.method_name = method_name

    def __enter__(self):
        for cls in self.klass.mro():
            self.__class__._stack[cls][self.method_name] += 1  # noqa: WPS437
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        for cls in self.klass.mro():
            self.__class__._stack[cls][self.method_name] -= 1  # noqa: WPS437
        return False

    @property
    def level(self) -> int:
        """Get level of inheritance"""
        stack = self.__class__._stack  # noqa: WPS437
        return stack[self.klass][self.method_name] - 1  # exclude current class
