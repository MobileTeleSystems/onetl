from __future__ import annotations

import logging
from contextlib import ExitStack, contextmanager
from functools import partial

from onetl.hooks.slot import Slot, is_slot, register_slot

logger = logging.getLogger(__name__)


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
    ---------

    Using as context manager:

    .. code:: python

        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg):
                ...


        @MyClass.my_method.hook
        def callback(self, arg):
            ...


        MyClass().my_method()  # will execute callback function

        with MyClass.skip_hooks():
            MyClass().my_method()  # will disable all hooks in the class

        MyClass().my_method()  # running outside the context restores previous behavior

    Using as decorator:

    .. code:: python

        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg):
                ...


        @MyClass.my_method.hook
        def callback(self, arg):
            ...


        def with_hook_enabled():
            MyClass().my_method()


        with_hook_enabled()  # will execute callback function


        @MyClass.skip_hooks()
        def with_all_hooks_disabled():
            MyClass().my_method()


        with_all_hooks_disabled()  # will disable all hooks in the class

        MyClass().my_method()  # running outside a decorated function restores previous behavior
    """

    slots = get_slots(cls)

    with ExitStack() as stack:
        for slot in slots:
            stack.enter_context(slot.skip_hooks())
        yield


def stop_hooks(cls: type) -> None:
    """
    Disables all hooks for all the methods of a specific class.

    Examples
    ---------

    .. code:: python

        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg):
                ...


        @MyClass.my_method.hook
        def callback(self, arg):
            ...


        MyClass().my_method()  # will execute callback function

        MyClass.stop_hooks()

        MyClass().my_method()  # will NOT execute callback
    """

    slots = get_slots(cls)

    for slot in slots:
        slot.stop_hooks()


def resume_hooks(cls: type) -> None:
    """
    Enables all hooks for all the methods of a specific class.

    Examples
    ---------

    .. code:: python

        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg):
                ...


        @MyClass.my_method.hook
        def callback(self, arg):
            ...


        MyClass.stop_hooks()
        MyClass().my_method()  # will NOT execute callback, it is disabled

        MyClass.resume_hooks()

        MyClass().my_method()  # will execute callback function
    """

    slots = get_slots(cls)

    for slot in slots:
        slot.resume_hooks()


def support_hooks(cls):
    """
    Decorator which adds hooks functionality to a specific class.

    Only methods decorated with :obj:`~slot` can be used for connecting hooks.

    Adds :obj:`~skip_hooks`, :obj:~stop_hooks` and :obj:`~resume_hooks` to the class.

    Examples
    ---------

    .. code:: python

        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg):
                ...


        @MyClass.my_method.hook
        def callback(self, arg):
            ...


        MyClass().my_method()  # will execute callback function
    """

    has_slots = False
    for method_name, method in cls.__dict__.items():
        if is_slot(method):
            has_slots = True
            setattr(cls, method_name, register_slot(cls, method_name))

    if not has_slots:
        raise SyntaxError("@support_hooks can be used only with @slot decorator on some of class methods")

    cls.skip_hooks = partial(skip_hooks, cls)
    cls.stop_hooks = partial(stop_hooks, cls)
    cls.resume_hooks = partial(resume_hooks, cls)
    return cls
