# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import inspect
import logging
import textwrap
from collections import defaultdict
from contextlib import ExitStack, suppress
from functools import partial, wraps
from typing import Any, Callable, ContextManager, TypeVar

from typing_extensions import Protocol

from onetl.exception import SignatureError
from onetl.hooks.hook import CanProcessResult, Hook, HookPriority
from onetl.hooks.hook_collection import HookCollection
from onetl.hooks.hooks_state import HooksState
from onetl.hooks.method_inheritance_stack import MethodInheritanceStack
from onetl.log import NOTICE

Method = TypeVar("Method", bound=Callable[..., Any])

logger = logging.getLogger(__name__)


def _unwrap_method(method: Method) -> Method:
    """Unwrap @classmethod and @staticmethod to get original function"""
    return getattr(method, "__func__", method)


def get_hooks(cls: type, method_name: str) -> HookCollection:
    """Return all hooks registered for a specific method"""

    method = _unwrap_method(cls.__dict__.get(method_name, None))
    return getattr(method, "__hooks__", HookCollection())


def get_hooks_hierarchy(cls: type, method_name: str) -> HookCollection:
    """
    Return all hooks registered for a specific method,
    sorted by priority and class nested level.

    See :ref:`hooks-design` for more details.
    """

    hooks_by_priority: dict[tuple[HookPriority, int], HookCollection] = defaultdict(HookCollection)

    # get all the hooks from the root class to the most nested one
    for nested_level, klass in enumerate(reversed(cls.mro())):
        if method_name not in dir(klass):
            continue

        for hook in get_hooks(klass, method_name).active:
            hooks_by_priority[(hook.priority, nested_level)].add(hook)

    result = HookCollection()
    for priority in sorted(hooks_by_priority):
        result.extend(hooks_by_priority[priority])

    return result


def bind_hook(method: Callable, inp=None):
    """
    Bind a hook to the slot.

    See :ref:`hooks-design` for more details.

    .. versionadded:: 0.7.0

    Examples
    --------

    .. code:: python

        from onetl.hooks import support_hooks, slot, hook, HookPriority


        @support_hooks
        class MyClass:
            @slot
            def method(self, arg):
                pass


        @MyClass.method.bind
        @hook
        def callable(self, arg):
            if arg == "some":
                do_something()


        @MyClass.method.bind
        @hook(priority=HookPriority.FIRST, enabled=True)
        def another_callable(self, arg):
            if arg == "another":
                raise NotAllowed()


        obj = MyClass()
        obj.method(1)  # will call both callable(obj, 1) and another_callable(obj, 1)
    """

    def inner_wrapper(hook):  # noqa: WPS430
        if not isinstance(hook, Hook):
            raise TypeError(
                f"@{method.__qualname__}.bind decorator can be used only on top function marked with @hook",
            )

        method.__hooks__.add(hook)

        logger.debug(
            "|onETL| Registered hook '%s.%s' for '%s' (enabled=%r, priority=%s)",
            hook.__module__,
            hook.__qualname__,  # type: ignore[attr-defined]
            method.__qualname__,
            hook.enabled,
            hook.priority,
        )

        return hook

    if inp is None:
        return inner_wrapper

    return inner_wrapper(inp)


def _prepare_hook_args(
    method: Callable,
    hook: Hook,
    args: tuple,
    kwargs: dict,
) -> inspect.BoundArguments:
    """
    Prepare args for calling hook using original method's args.

    For example, if method has signature:

    .. code:: python

        @support_hooks
        class MyClass:
            @slot
            def method(self, some, named="abc"): ...

    then hook should have a compatible signature, like these ones:

    .. code:: python

        @MyClass.method.bind
        @hook
        def callback(self, some, named): ...

    .. code:: python

        @MyClass.method.bind
        @hook
        def callback(self, some, **kwargs): ...

    .. code:: python

        @MyClass.method.bind
        @hook
        def callback(my_class_instance, *args, **kwargs): ...

    .. note::

        If hook signature contains arg with name ``method_name``, slot method name will be passed into it:

        .. code:: python

            @MyClass.method.bind
            @hook
            def callback(self, method_name, *args, **kwargs):
                assert method_name == "method"

        This can be useful for adding multiple hooks on the same callback function.
    """
    try:
        hook_signature = inspect.signature(hook.callback)
        if "method_name" in hook_signature.parameters:
            return hook_signature.bind(
                *args,
                method_name=method.__name__,
                **kwargs,
            )

        return hook_signature.bind(*args, **kwargs)
    except TypeError as e:
        method_source_line = 0
        with suppress(OSError):
            method_source_line = inspect.getsourcelines(method)[1]

        hook_source_line = 0
        with suppress(OSError):
            hook_source_line = inspect.getsourcelines(hook.callback)[1]

        raise SignatureError(
            textwrap.dedent(
                f"""
                Error while passing method arguments to a hook.

                Method name: '{method.__module__}.{method.__qualname__}'
                Method source: '{inspect.getsourcefile(method)}:{method_source_line}'
                Method signature:
                    {inspect.signature(method)}

                Hook name: '{hook.callback.__module__}.{hook.callback.__qualname__}'
                Hook source: '{inspect.getsourcefile(hook.callback)}:{hook_source_line}'
                Hook signature:
                    {inspect.signature(hook.callback)}
                """,
            ),
        ) from e


def _execute_hook(hook: Hook, args: inspect.BoundArguments):
    """
    Executes the hook using passed arguments, and returns call result.
    """
    try:
        return hook(*args.args, **args.kwargs)
    except Exception:
        logger.exception(
            "|Hooks| Error while executing a hook '%s.%s'",
            hook.__module__,
            hook.__qualname__,  # type: ignore[attr-defined]
        )
        raise


def _handle_context_result(result: Any, context: CanProcessResult, hook: Hook):
    """
    Calls ``context.process_result(result)`` and handles exception, if any.

    See :obj:`onetl.hooks.hook.hook` for more details.
    """
    try:
        return context.process_result(result)
    except Exception:
        logger.exception(
            "|Hooks| Error while passing method call result to the hook '%s.%s'",
            hook.__module__,
            hook.__qualname__,  # type: ignore[attr-defined]
        )
        raise


def register_slot(cls: type, method_name: str):  # noqa: WPS231, WPS213, WPS212
    """
    Internal callback to register ``SomeClass.some_method`` as a slot.

    It is not applied as decorator because decorators have no access to the class but only to a specific method.

    Also ``@classmethod`` is a descriptor, and it can be called only my accessing the class itself,
    which is not possible within a decorator.

    .. versionadded:: 0.7.0

    Examples
    --------

    .. code:: python

        class MyClass:
            def method(self, arg1):
                pass

            @classmethod
            def class_method(cls):
                pass

            @staticmethod
            def static_method(arg1, arg2):
                pass


        MyClass.method = register_slot(MyClass, "method")
        MyClass.class_method = register_slot(MyClass, "class_method")
        MyClass.static_method = register_slot(MyClass, "static_method")
    """

    # <function MyClass.method>
    # <classmethod object>
    # <staticmethod object>
    method_or_descriptor = cls.__dict__[method_name]

    # <function MyClass.method>
    # <function MyClass.class_method>
    # <function MyClass.static_method>
    original_method = _unwrap_method(method_or_descriptor)

    @wraps(original_method)  # noqa: WPS231, WPS213
    def wrapper(*args, **kwargs):  # noqa: WPS231, WPS213
        with MethodInheritanceStack(cls, method_name) as stack_manager, ExitStack() as context_stack:
            if not HooksState.enabled():
                logger.log(NOTICE, "|Hooks| All hooks are disabled")
                return original_method(*args, **kwargs)

            if stack_manager.level > 0:
                logger.log(
                    NOTICE,
                    "|Hooks| Skipping hooks for '%s' because method is called via super()",
                    original_method.__qualname__,
                )
                return original_method(*args, **kwargs)

            hooks = get_hooks_hierarchy(cls, method_name)
            if not hooks:
                logger.log(NOTICE, "|Hooks| No active hooks for '%s'", original_method.__qualname__)
                return original_method(*args, **kwargs)

            original_indent = 4 * stack_manager.level
            indent = original_indent
            context_results: list[tuple[Hook, Any]] = []
            before_results: list[tuple[Hook, Any]] = []

            hooks_count = len(hooks)
            if hooks_count > 1:
                logger.log(
                    NOTICE,
                    "|Hooks| %s%s hooks registered for '%s.%s'",
                    " " * indent,
                    hooks_count,
                    cls.__qualname__,
                    method_name,
                )

            for i, hook in enumerate(hooks):
                logger.log(
                    NOTICE,
                    "|Hooks| %sCalling hook '%s.%s' (%d of %d)",
                    " " * indent,
                    hook.__module__,
                    hook.__qualname__,  # type: ignore[attr-defined]
                    i + 1,
                    hooks_count,
                )

                hook_args = _prepare_hook_args(
                    method=original_method,
                    hook=hook,
                    args=args,
                    kwargs=kwargs,
                )

                hook_call_result = _execute_hook(hook, hook_args)

                if isinstance(hook_call_result, ContextManager):
                    logger.log(
                        NOTICE,
                        "|Hooks| %sThis is a context manager, entering...",
                        " " * indent,
                    )
                    indent += 2
                    context_results.append((hook, hook_call_result))
                    context_stack.enter_context(hook_call_result)
                elif hook_call_result is not None:
                    before_results.append((hook, hook_call_result))
                    logger.log(
                        NOTICE,
                        "|Hooks| %sHook is finished with returning non-None result",
                        " " * indent,
                    )
                else:
                    logger.log(
                        NOTICE,
                        "|Hooks| %sHook is finished with None result",
                        " " * indent,
                    )

            logger.log(
                NOTICE,
                "|Hooks| %sCalling original method '%s.%s.%s'",
                " " * indent,
                cls.__module__,
                cls.__qualname__,
                method_name,
            )

            result = original_method(*args, **kwargs)
            call_result = "None result" if result is None else "non-None result"

            logger.log(
                NOTICE,
                "|Hooks| %sMethod call is finished with %s",
                " " * indent,
                call_result,
            )

            indent = original_indent

            if before_results:
                before_hook, before_result = before_results[-1]

                call_result = "(None)" if result is None else "(*NOT None*)"
                logger.log(
                    NOTICE,
                    "|Hooks| %sMethod call result %s will be replaced with result of calling the hook '%s.%s'",
                    " " * indent,
                    call_result,
                    before_hook.__module__,
                    before_hook.__qualname__,  # type: ignore[attr-defined]
                )
                result = before_result

            for hook, context in context_results:
                if isinstance(context, CanProcessResult):
                    logger.log(
                        NOTICE,
                        "|Hooks| %sPassing result to 'process_result' method of context manager '%s.%s'",
                        " " * indent,
                        hook.__module__,
                        hook.__qualname__,  # type: ignore[attr-defined]
                    )
                    context_result = _handle_context_result(
                        result=result,
                        context=context,
                        hook=hook,
                    )

                    if context_result is not None:
                        call_result = "(None)" if result is None else "(*NOT* None)"  # noqa: WPS220
                        logger.log(  # noqa: WPS220
                            NOTICE,
                            "|Hooks| %sMethod call result %s is modified by hook!",
                            " " * indent,
                            call_result,
                        )
                        result = context_result  # noqa: WPS220
                indent += 2

            return result

    # adding __hooks__ to wrapper is important to detect them with @support_hooks decorator
    wrapper.__hooks__ = method_or_descriptor.__hooks__  # type: ignore[attr-defined]

    wrapper.skip_hooks = wrapper.__hooks__.skip  # type: ignore[attr-defined]
    wrapper.suspend_hooks = wrapper.__hooks__.stop  # type: ignore[attr-defined]
    wrapper.resume_hooks = wrapper.__hooks__.resume  # type: ignore[attr-defined]
    wrapper.bind = partial(bind_hook, wrapper)  # type: ignore[attr-defined]

    # wrap result back to @classmethod and @staticmethod, if was used
    if isinstance(method_or_descriptor, classmethod):
        return classmethod(wrapper)
    elif isinstance(method_or_descriptor, staticmethod):
        return staticmethod(wrapper)

    return wrapper


def _is_method(method: Callable) -> bool:
    """Checks whether class method is callable"""
    method = _unwrap_method(method)
    return callable(method)


def _is_private(method: Callable) -> bool:
    """Checks whether class method is private"""
    method_name = _unwrap_method(method).__name__
    return method_name.startswith("_") and not method_name.endswith("__")


def is_slot(method: Callable) -> bool:
    """Returns True if method is a hook slot"""
    return hasattr(method, "__hooks__")


class Slot(Protocol):
    """Protocol which is implemented by a method after applying :obj:`~slot` decorator.

    .. versionadded:: 0.7.0
    """

    def __call__(self, *args, **kwargs): ...

    @property
    def __hooks__(self) -> HookCollection:
        """Collection of hooks bound to the slot"""

    def skip_hooks(self) -> ContextManager[None]:
        """
        Context manager which temporary stops all the hooks bound to the slot.

        .. note::

            If hooks were stopped by :obj:`~suspend_hooks`, they will not be resumed
            after exiting the context/decorated function.
            You should call :obj:`~resume_hooks` explicitly.

        Examples
        --------

        .. tabs::

            .. code-tab:: py Context manager syntax

                from onetl.hooks.hook import hook, support_hooks, slot


                @support_hooks
                class MyClass:
                    @slot
                    def my_method(self, arg):
                        ...


                @MyClass.my_method.bind
                @hook
                def callback1(self, arg):
                    ...

                obj = MyClass()
                obj.my_method(1)  # will call callback1(obj, 1)

                with MyClass.my_method.skip_hooks():
                    obj.my_method(1)  # will NOT call callback1

                obj.my_method(2)  # will call callback1(obj, 2)

            .. code-tab:: py Decorator syntax

                from onetl.hooks.hook import hook, support_hooks, slot


                @support_hooks
                class MyClass:
                    @slot
                    def my_method(self, arg):
                        ...


                @MyClass.my_method.bind
                @hook
                def callback1(self, arg):
                    ...

                @MyClass.my_method.skip_hooks()
                def method_without_hooks(obj, arg):
                    obj.my_method(arg)


                obj = MyClass()
                obj.my_method(1)  # will call callback1(obj, 1)

                method_without_hooks(obj, 1)  # will NOT call callback1

                obj.my_method(2)  # will call callback1(obj, 2)
        """

    def suspend_hooks(self):
        """
        Stop all the hooks bound to the slot.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import hook, support_hooks, slot


            @support_hooks
            class MyClass:
                @slot
                def my_method(self, arg): ...


            @MyClass.my_method.bind
            @hook
            def callback1(self, arg): ...


            obj = MyClass()
            obj.my_method(1)  # will call callback1(obj, 1)

            MyClass.my_method.suspend_hooks()
            obj.my_method(1)  # will NOT call callback1
        """

    def resume_hooks(self):
        """
        Resume all hooks bound to the slot.

        .. note::

            If hook is disabled by :obj:`onetl.hooks.hook.Hook.disable`, it will stay disabled.
            You should call :obj:`onetl.hooks.hook.Hook.enable` explicitly.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import hook, support_hooks, slot


            @support_hooks
            class MyClass:
                @slot
                def my_method(self, arg): ...


            @MyClass.my_method.bind
            @hook
            def callback1(self, arg): ...


            obj = MyClass()
            obj.my_method(1)  # will call callback1(obj, 1)

            MyClass.my_method.suspend_hooks()
            obj.my_method(1)  # will NOT call callback1

            MyClass.my_method.resume_hooks()
            obj.my_method(2)  # will call callback1(obj, 2)
        """

    @wraps(bind_hook)
    def bind(self): ...


def slot(method: Method) -> Method:
    """
    Decorator which enables hooks functionality on a specific class method.

    Decorated methods get additional nested methods:

        * :obj:`onetl.hooks.slot.Slot.bind`
        * :obj:`onetl.hooks.slot.Slot.suspend_hooks`
        * :obj:`onetl.hooks.slot.Slot.resume_hooks`
        * :obj:`onetl.hooks.slot.Slot.skip_hooks`

    .. note::

        Supported method types are:

            * Regular methods
            * ``@classmethod``
            * ``@staticmethod``

        It is not allowed to use this decorator over ``_private`` and ``__protected`` methods and ``@property``.
        But is allowed to use on ``__dunder__`` methods, like ``__init__``.

    .. versionadded:: 0.7.0

    Examples
    --------

    .. code:: python

        from onetl.hooks import support_hooks, slot, hook


        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg): ...

            @slot  # decorator should be on top of all other decorators
            @classmethod
            def class_method(cls): ...

            @slot  # decorator should be on top of all other decorators
            @staticmethod
            def static_method(arg): ...


        @MyClass.my_method.bind
        @hook
        def callback1(self, arg): ...


        @MyClass.class_method.bind
        @hook
        def callback2(cls): ...


        @MyClass.static_method.bind
        @hook
        def callback3(arg): ...


        obj = MyClass()
        obj.my_method(1)  # will execute callback1(obj, 1)
        MyClass.class_method(2)  # will execute callback2(MyClass, 2)
        MyClass.static_method(3)  # will execute callback3(3)
    """

    if hasattr(method, "__hooks__"):
        raise SyntaxError("Cannot place @slot hook twice on the same method")

    original_method = getattr(method, "__wrapped__", method)

    if not _is_method(original_method):
        raise TypeError(f"@slot decorator could be applied to only to methods of class, got {type(original_method)}")

    if _is_private(original_method):
        raise ValueError(f"@slot decorator could be applied to public methods only, got '{original_method.__name__}'")

    method.__hooks__ = HookCollection()  # type: ignore[attr-defined]
    return method
