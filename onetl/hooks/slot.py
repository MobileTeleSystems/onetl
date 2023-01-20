from __future__ import annotations

import inspect
import logging
import textwrap
from collections import defaultdict
from contextlib import ExitStack, suppress
from functools import partial, wraps
from types import FunctionType
from typing import Any, Callable, ContextManager, TypeVar

from typing_extensions import Protocol

from onetl.exception import SignatureError
from onetl.hooks.hook import CanHandleResult, Hook, HookPriority
from onetl.hooks.hook_collection import HookCollection
from onetl.hooks.hooks_state import HooksState
from onetl.hooks.method_inheritance_stack import MethodInheritanceStack
from onetl.log import NOTICE

logger = logging.getLogger(__name__)

T = TypeVar("T")


class Slot(Protocol):
    """Protocol which is implemented by a method after applying :obj:`~slot` decorator."""

    def __call__(self, *args, **kwargs):
        ...

    @property
    def __hooks__(self) -> HookCollection:
        ...

    def skip_hooks(self):
        ...

    def stop_hooks(self):
        ...

    def resume_hooks(self):
        ...


def get_hooks(cls: type, method_name: str) -> HookCollection:
    """Return all hooks registered for a specific method"""

    method = cls.__dict__[method_name]
    return getattr(method, "__hooks__", HookCollection())


def get_hooks_hierarchy(cls: type, method_name: str) -> HookCollection:
    """
    Return all hooks registered for a specific method,
    sorted by priority and class nested level.

    Hooks are executed in the following order:

        * ``ParentClass``, ``FIRST``
        * ``NestedClass``, ``FIRST``
        * ``ParentClass``, ``MIDDLE``
        * ``NestedClass``, ``MIDDLE``
        * ``ParentClass``, ``LAST``
        * ``NestedClass``, ``LAST``

    See :obj:`onetl.hooks.hook.HookPriority` for more details.
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


def connect_hook(method: Hook, inp=None):
    """
    Connect hook to a given slot.

    See :ref:`hooks` for more details.

    Examples
    --------

    .. code:: python

        from onetl.hooks import support_hooks, slot, HookPriority


        @support_hooks
        class MyClass:
            @slot
            def method(self, arg):
                pass


        @MyClass.method.hook
        def hook(self, arg):
            if arg == "some":
                do_something()


        @MyClass.method.hook(priority=HookPriority.FIRST, enabled=True)
        def another_hook(self, arg):
            if arg == "another":
                raise NotAllowed()
    """

    def inner_wrapper(hook):  # noqa: WPS430
        if not isinstance(hook, Hook):
            raise TypeError(
                f"@{method.__qualname__}.connect decorator can be used only on top function marked with @hook",
            )

        method.__hooks__.add(hook)

        logger.debug(
            "|onETL| Registered hook '%s.%s' for '%s' (enabled=%r, priority=%s)",
            hook.__module__,
            hook.__qualname__,
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
    prefix_args: tuple,
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
            def method(self, some, named="abc"):
                ...

    then hook should have a compatible signature, like these ones:

    .. code:: python

        @MyClass.method.hook
        def callback(self, some, named):
            ...

    .. code:: python

        @MyClass.method.hook
        def callback(self, some, **kwargs):
            ...

    .. code:: python

        @MyClass.method.hook
        def callback(my_class_instance, *args, **kwargs):
            ...

    .. note::

        If hook signature contains arg with name ``method_name``, slot method name will be passed into it:

        .. code:: python

            @MyClass.method.hook
            def callback(self, method_name, *args, **kwargs):
                assert method_name == "method"

        This can be useful for adding multiple hooks on the same callback function.
    """
    try:
        hook_signature = inspect.signature(hook.callback)
        if "method_name" in hook_signature.parameters:
            return hook_signature.bind(
                *prefix_args,
                method_name=method.__name__,
                *args,
                **kwargs,
            )

        return hook_signature.bind(*prefix_args, *args, **kwargs)
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

                Hook name: '{hook.__module__}.{hook.__qualname__}'
                Hook source: '{inspect.getsourcefile(hook.callback)}:{hook_source_line}'
                Hook signature:
                    {inspect.signature(hook.callback)}
                """,  # type: ignore[attr-defined]
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


def _handle_context_result(result: Any, context: CanHandleResult, hook: Hook):
    """
    Calls ``context.handle_result(result)`` and handles exception, if any.

    See :obj:`onetl.hooks.hook.hook` for more details.
    """
    try:
        return context.handle_result(result)
    except Exception:
        logger.exception(
            "|Hooks| Error while passing method call result to the hook '%s.%s'",
            hook.__module__,
            hook.__qualname__,  # type: ignore[attr-defined]
        )
        raise


def register_slot(cls: type, method_name: str):  # noqa: WPS231, WPS213
    """
    Internal callback to register ``SomeClass.some_method`` as a slot.

    It is not applied as decorator because decorators have no access to the class but only to a specific method.

    Also ``@classmethod`` is a descriptor, and it can be called only my accessing the class itself,
    which is not possible within a decorator.

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

    method = getattr(cls, method_name)
    raw_method = cls.__dict__[method_name]
    is_classmethod = isinstance(raw_method, classmethod)

    @wraps(method)  # noqa: WPS231, WPS213
    def wrapper(*args, **kwargs):
        with MethodInheritanceStack(cls, method_name) as stack_manager, ExitStack() as context_stack:
            if not HooksState.enabled():
                logger.log(NOTICE, "|Hooks| All hooks are disabled")
                return method(*args, **kwargs)

            if stack_manager.level > 0:
                logger.log(
                    NOTICE,
                    "|Hooks| Skipping hooks for '%s' because method is called via super()",
                    method.__qualname__,
                )
                return method(*args, **kwargs)

            hooks = get_hooks_hierarchy(cls, method_name)
            if not hooks:
                logger.log(NOTICE, "|Hooks| No active hooks for '%s'", method.__qualname__)
                return method(*args, **kwargs)

            # Due to descriptor nature of @classmethod, when it is being called, `cls` argument
            # is not being passed in `args`. Instead, descriptor already knows which class it is bounded to.
            # So we need to pass it explicitly because hook is not a descriptor
            prefix_args = (cls,) if is_classmethod else ()

            indent = 4 * stack_manager.level
            context_results: list[tuple[Hook, Any]] = []
            before_results: list[tuple[Hook, Any]] = []

            if len(hooks) > 1:
                logger.log(
                    NOTICE,
                    "|Hooks| %s%s hooks registered for '%s.%s'",
                    " " * indent,
                    len(hooks),
                    cls.__qualname__,
                    method_name,
                )

            for hook in hooks:
                logger.log(
                    NOTICE,
                    "|Hooks| %sCalling hook '%s.%s'",
                    " " * indent,
                    hook.__module__,
                    hook.__qualname__,
                )

                hook_args = _prepare_hook_args(
                    method=method,
                    hook=hook,
                    prefix_args=prefix_args,
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
                        "|Hooks| %sHook is finished with returning some result",
                        " " * indent,
                    )
                else:
                    logger.log(
                        NOTICE,
                        "|Hooks| %sHook is finished with empty result",
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

            result = method(*args, **kwargs)

            logger.log(
                NOTICE,
                "|Hooks| %sMethod call is finished",
                " " * indent,
            )

            if before_results:
                before_hook, before_result = before_results[-1]

                call_result = "(None)" if result is None else "(*NOT None*)"
                logger.log(
                    NOTICE,
                    "|Hooks| %sMethod call result %s will be replaced with result of hook '%s.%s'",
                    " " * indent,
                    call_result,
                    before_hook.__module__,
                    before_hook.__qualname__,
                )
                result = before_result

            for hook, context in context_results:
                indent -= 2
                if isinstance(context, CanHandleResult):
                    logger.log(
                        NOTICE,
                        "|Hooks| %sPassing method call result to 'handle_result' method of context manager '%s.%s'",
                        " " * indent,
                        hook.__module__,
                        hook.__qualname__,
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
                            "|Hooks| %sMethod call result %s is modified by hook",
                            " " * indent,
                            call_result,
                        )
                        result = context_result  # noqa: WPS220

            return result

    # adding __hooks__ to wrapper is important to detect them with @support_hooks decorator
    wrapper.__hooks__ = raw_method.__hooks__  # type: ignore[attr-defined]

    wrapper.skip_hooks = wrapper.__hooks__.skip  # type: ignore[attr-defined]
    wrapper.stop_hooks = wrapper.__hooks__.stop  # type: ignore[attr-defined]
    wrapper.resume_hooks = wrapper.__hooks__.resume  # type: ignore[attr-defined]
    wrapper.connect = partial(connect_hook, wrapper)  # type: ignore[attr-defined]
    return wrapper


def _is_method(method: Callable) -> bool:
    """Checks whether class method is callable"""
    return isinstance(method, (FunctionType, classmethod, staticmethod))


def _is_private(method: Callable) -> bool:
    """Checks whether class method is private"""
    method_name = getattr(method, "__func__", method).__name__
    return method_name.startswith("_") and not method_name.endswith("__")


def is_slot(method: Callable) -> bool:
    """Returns True if method is a hook slot"""
    return hasattr(method, "__hooks__")


def slot(method) -> Slot:
    """
    Decorator which enables hooks functionality on a specific class method.

    Decorated methods get additional nested methods:

        * :obj:`~stop_hooks`
        * :obj:`~resume_hooks`
        * :obj:`~skip_hooks`

    Supported method types are:

        * Regular methods
        * ``@classmethod``
        * ``@staticmethod``

    It is not allowed to use this decorator over ``_private`` and ``__protected`` methods and ``@property``.
    But is allowed to use on ``__dunder__`` methods, like ``__init__``.

    Examples
    ---------

    .. code:: python

        @support_hooks
        class MyClass:
            @slot
            def my_method(self, arg):
                ...

            @slot  # decorator should be on top of all other decorators
            @classmethod
            def class_method(cls):
                ...

            @slot  # decorator should be on top of all other decorators
            @staticmethod
            def static_method(arg):
                ...


        @MyClass.my_method.hook
        def callback1(self, arg):
            ...


        @MyClass.class_method.hook
        def callback2(cls):
            ...


        @MyClass.static_method.hook
        def callback3(arg):
            ...


        MyClass().my_method()  # will execute callback1 function
        MyClass.class_method()  # will execute callback2 function
        MyClass.static_method()  # will execute callback3 function
    """

    if hasattr(method, "__hooks__"):
        raise SyntaxError("Cannot place @slot hook twice on the same method")

    method = getattr(method, "__wrapped__", method)
    if not _is_method(method):
        raise TypeError(f"@slot decorator could be applied to only to methods of class, got {type(method)}")

    if _is_private(method):
        raise ValueError(f"@slot decorator could be applied to public methods only, got '{method.__name__}'")

    method.__hooks__ = HookCollection()
    return method
