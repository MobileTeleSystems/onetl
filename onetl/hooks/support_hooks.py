# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from contextlib import ExitStack, contextmanager
from functools import partial
from typing import TypeVar

from onetl.hooks.slot import Slot, is_slot, register_slot

logger = logging.getLogger(__name__)

Klass = TypeVar("Klass", bound=type)


def get_slots(cls: type) -> list[Slot]:
    """Returns list of methods in class decorated with :obj:`~slot`"""
    result = []
    for method_name in dir(cls):
        method = getattr(cls, method_name)
        if is_slot(method):
            result.append(method)

    return result


@contextmanager
def skip_hooks(cls: type):
    """
    Context manager (and decorator) which temporary disables hooks for all the methods of a specific class.

    Examples
    --------

    .. tabs::

        .. code-tab:: py Context manager syntax

            @support_hooks
            class MyClass:
                @slot
                def my_method(self, arg):
                    ...


            @MyClass.my_method.hook
            def callback(self, arg):
                ...


            obj = MyClass()
            obj.my_method(1)  # will execute callback(obj, 1)

            with MyClass.skip_hooks():
                obj.my_method()  # will NOT execute callback

            # running outside the context restores previous behavior
            obj.my_method(2)  # will execute callback(obj, 2)

        .. code-tab:: py Decorator syntax

            @support_hooks
            class MyClass:
                @slot
                def my_method(self, arg):
                    ...


            @MyClass.my_method.hook
            def callback(self, arg):
                ...


            def with_hook_enabled():
                obj = MyClass()
                obj.my_method(1)


            with_hook_enabled()  # will execute callback(obj, 1)


            @MyClass.skip_hooks()
            def with_all_hooks_disabled():
                obj = MyClass()
                obj.my_method(1)


            with_all_hooks_disabled()  # will NOT execute callback function

            # running outside a decorated function restores previous behavior
            obj = MyClass()
            obj.my_method(2)  # will execute callback(obj, 2)

    .. versionadded:: 0.7.0
    """

    slots = get_slots(cls)

    with ExitStack() as stack:
        for slot in slots:
            stack.enter_context(slot.skip_hooks())
        yield


def suspend_hooks(cls: type) -> None:
    """
    Disables all hooks for all the methods of a specific class.

    Examples
    --------

    .. code:: python

        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg): ...


        @MyClass.my_method.hook
        def callback(self, arg): ...


        obj = MyClass()
        obj.my_method(1)  # will execute callback(obj, 1)

        MyClass.suspend_hooks()

        obj.my_method(2)  # will NOT execute callback

    .. versionadded:: 0.7.0
    """

    slots = get_slots(cls)

    for slot in slots:
        slot.suspend_hooks()


def resume_hooks(cls: type) -> None:
    """
    Enables all hooks for all the methods of a specific class.

    Examples
    --------

    .. code:: python

        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg): ...


        @MyClass.my_method.hook
        def callback(self, arg): ...


        obj = MyClass()

        MyClass.suspend_hooks()
        obj.my_method(1)  # will NOT execute callback

        MyClass.resume_hooks()

        obj.my_method(2)  # will execute callback(obj, 2)

    .. versionadded:: 0.7.0
    """

    slots = get_slots(cls)

    for slot in slots:
        slot.resume_hooks()


def support_hooks(cls: Klass) -> Klass:
    """
    Decorator which adds hooks functionality to a specific class.

    Only methods decorated with :obj:`~slot` can be used for connecting hooks.

    Adds :obj:`~skip_hooks`, :obj:`~suspend_hooks` and :obj:`~resume_hooks` to the class.

    .. versionadded:: 0.7.0

    Examples
    --------

    .. code:: python

        from onetl.hooks.hook import support_hooks, slot


        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg): ...


        @MyClass.my_method.hook
        def callback(self, arg): ...


        MyClass().my_method()  # will execute callback function
    """

    has_slots = False
    for method_name, method in cls.__dict__.items():
        if is_slot(method):
            has_slots = True
            setattr(cls, method_name, register_slot(cls, method_name))

    if not has_slots:
        raise SyntaxError("@support_hooks can be used only with @slot decorator on some of class methods")

    cls.skip_hooks = partial(skip_hooks, cls)  # type: ignore[attr-defined]
    cls.suspend_hooks = partial(suspend_hooks, cls)  # type: ignore[attr-defined]
    cls.resume_hooks = partial(resume_hooks, cls)  # type: ignore[attr-defined]
    return cls
