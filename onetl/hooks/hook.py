# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Generic, ParamSpec, Protocol, TypeVar, overload, runtime_checkable

from onetl.log import NOTICE

logger = logging.getLogger(__name__)

T = TypeVar("T")
P = ParamSpec("P")


class HookPriority(int, Enum):
    """
    Hook priority enum.

    All hooks within the same priority are executed in the same order they were registered.

    !!! success "Added in 0.7.0"
    """

    FIRST = -1
    "Hooks with this priority will run first."

    NORMAL = 0
    "Hooks with this priority will run after [onetl.hooks.hook.HookPriority.FIRST][] but before [onetl.hooks.hook.HookPriority.LAST][]."

    LAST = 1
    "Hooks with this priority will run last."


@dataclass
class Hook(Generic[P, T]):
    """
    Hook representation.

    !!! success "Added in 0.7.0"

    Parameters
    ----------
    callback

        Some callable object which will be wrapped into a Hook, like function or ContextManager class.

    enabled

        Will hook be executed or not. Useful for debugging.

    priority

        Changes hooks priority, see `HookPriority` documentation.

    Examples
    --------

    ```python
    from onetl.hooks.hook import Hook, HookPriority


    def some_func(*args, **kwargs): ...


    hook = Hook(callback=some_func, enabled=True, priority=HookPriority.FIRST)

    ```
    """

    callback: Callable[P, T]
    enabled: bool = True
    priority: HookPriority = HookPriority.NORMAL

    def __post_init__(self):
        self.priority = HookPriority(self.priority)

    def enable(self):
        """
        Enable the hook.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> def func1(): ...
        >>> hook = Hook(callback=func1, enabled=False)
        >>> hook.enabled
        False
        >>> hook.enable()
        >>> hook.enabled
        True

        ```
        """
        if self.enabled:
            logger.log(
                NOTICE,
                "|Hooks| Hook '%s.%s' already enabled",
                self.callback.__module__,
                self.callback.__qualname__,
            )
        else:
            logger.log(NOTICE, "|Hooks| Enable hook '%s.%s'", self.callback.__module__, self.callback.__qualname__)
            self.enabled = True

    def disable(self):
        """
        Disable the hook.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> def func1(): ...
        >>> hook = Hook(callback=func1, enabled=True)
        >>> hook.enabled
        True
        >>> hook.disable()
        >>> hook.enabled
        False

        ```
        """
        if self.enabled:
            logger.log(NOTICE, "|Hooks| Disable hook '%s.%s'", self.callback.__module__, self.callback.__qualname__)
            self.enabled = False
        else:
            logger.log(
                NOTICE,
                "|Hooks| Hook '%s.%s' already disabled",
                self.callback.__module__,
                self.callback.__qualname__,
            )

    @contextmanager
    def skip(self):
        """
        Temporary disable the hook.

        !!! note

            If hook was created with `enabled=False`, or was disabled by [disable][],
            its state will left intact after exiting the context.

            You should call [enable][] explicitly to change its state.

        !!! success "Added in 0.7.0"

        Examples
        --------

        === "Context manager syntax"
            ```python
            >>> def func1(): ...
            >>> hook = Hook(callback=func1, enabled=True)
            >>> hook.enabled
            True
            >>> with hook.skip():
            ...     print(hook.enabled)
            False
            >>> # hook state is restored as it was before entering the context manager
            >>> hook.enabled
            True

            ```

        === "Decorator syntax"
            ```python
            >>> def func1(): ...
            >>> hook = Hook(callback=func1, enabled=True)
            >>> hook.enabled
            True
            >>> @hook.skip()
            ... def hook_disabled():
            ...     print(hook.enabled)
            >>> hook_disabled()
            False
            >>> # hook state is restored as it was before entering the context manager
            >>> hook.enabled
            True

            ```
        """
        if not self.enabled:
            logger.log(
                NOTICE,
                "|Hooks| Hook '%s.%s' already disabled, nothing to skip",
                self.callback.__module__,
                self.callback.__qualname__,
            )
            yield
        else:
            logger.log(
                NOTICE,
                "|Hooks| Skipping hook '%s.%s' ...",
                self.callback.__module__,
                self.callback.__qualname__,
            )
            self.enabled = False

            yield

            logger.log(
                NOTICE,
                "|Hooks| Restoring hook '%s.%s' ...",
                self.callback.__module__,
                self.callback.__qualname__,
            )
            self.enabled = True

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """
        Calls the original callback with passed args.

        Examples
        --------

        ```python
        >>> from onetl.hooks.hook import Hook, HookPriority
        >>> def some_func(*args, **kwargs):
        ...     print(args)
        ...     print(kwargs)
        ...     return "func result"
        >>> hook = Hook(callback=some_func)
        >>> result = hook(1, "abc", some="arg")
        (1, 'abc')
        {'some': 'arg'}
        >>> result
        'func result'

        ```
        """
        result = self.callback(*args, **kwargs)
        if isinstance(result, Generator):
            return ContextDecorator(result)  # type: ignore[return-value]
        return result


@runtime_checkable
class CanProcessResult(Protocol):
    """
    Protocol which should be implemented by ContextManager
    allowing it to process result of original method call and modify/replace it with something else.
    """

    def process_result(self, result: T) -> T: ...


class ContextDecorator:
    """
    Helper for [hook][] decorator.

    Analog of `contextlib._GeneratorContextManager`, but instead of generator function
    plus arguments it accepts generator object.

    Also it does not implement `__call__`, so it does not allow for context manager to be
    used as a decorator.
    """

    def __init__(self, gen: Generator):
        self.gen = gen
        self.first_yield_result = None

    def __enter__(self):
        """
        Start generator and stop at first `yield`.

        If generator instead of:

        ```python
        result = yield
        ...
        yield result

        ```
        looks like:

        ```python
        yield something

        ```
        Just remember this output and return it in [process_result][] as is.
        """

        with suppress(StopIteration):
            self.first_yield_result = self.gen.send(None)

        return self

    def __exit__(self, typ, value, traceback):
        """
        Copy of `contextlib._GeneratorContextManager.__exit__`
        """
        if typ is None:
            # Faster way to run next(self.gen) and check for StopIteration:
            for _ in self.gen:
                try:
                    msg = "generator didn't stop"
                    raise RuntimeError(msg)
                finally:
                    self.gen.close()
            return False

        if value is None:
            # Need to force instantiation so we can reliably
            # tell if we get the same exception back
            value = typ()

        try:
            self.gen.throw(typ, value, traceback)
        except StopIteration as exc:
            # Suppress StopIteration *unless* it's the same exception that
            # was passed to throw().  This prevents a StopIteration
            # raised inside the "with" statement from being suppressed.
            return exc is not value
        except RuntimeError as exc:
            # Don't re-raise the passed in exception. (issue27122)
            if exc is value:
                exc.__traceback__ = traceback
                return False
            # Avoid suppressing if a StopIteration exception
            # was passed to throw() and later wrapped into a RuntimeError
            # (see PEP 479 for sync generators; async generators also
            # have this behavior). But do this only if the exception wrapped
            # by the RuntimeError is actually Stop(Async)Iteration (see
            # issue29692).
            if isinstance(value, StopIteration) and exc.__cause__ is value:
                value.__traceback__ = traceback
                return False
            raise
        except BaseException as exc:
            # only re-raise if it's *not* the exception that was
            # passed to throw(), because __exit__() must not raise
            # an exception unless __exit__() itself failed.  But throw()
            # has to raise the exception to signal propagation, so this
            # fixes the impedance mismatch between the throw() protocol
            # and the __exit__() protocol.
            if exc is not value:
                raise
            exc.__traceback__ = traceback
            return False

        try:
            msg = "generator didn't stop after throw()"
            raise RuntimeError(msg)
        finally:
            self.gen.close()

    def process_result(self, result):
        """
        Handle original method call result, and return new value.

        If generator instead of:

        ```python
        result = yield
        # this is there `process_result` is called
        yield result

        ```
        looks like:

        ```python
        yield something

        ```
        Just return the yielded result.
        """

        try:
            generator_result = self.gen.send(result)
            if generator_result is not None:
                return generator_result
        except StopIteration:
            return self.first_yield_result

        return None


@overload
def hook(
    inp: Callable[P, T], *, enabled: bool = True, priority: HookPriority = HookPriority.NORMAL
) -> Callable[P, T]: ...


@overload
def hook(
    inp: None, *, enabled: bool = True, priority: HookPriority = HookPriority.NORMAL
) -> Callable[[Callable[P, T]], Callable[P, T]]: ...


def hook(inp=None, *, enabled=True, priority=HookPriority.NORMAL):
    """
    Initialize hook from callable/context manager.

    !!! success "Added in 0.7.0"

    Examples
    --------

    === "Decorate a function or generator"

        ```python
        from onetl.hooks import hook, HookPriority


        @hook
        def some_func(*args, **kwargs):
            ...


        @hook(enabled=True, priority=HookPriority.FIRST)
        def another_func(*args, **kwargs):
            ...

        ```

    === "Decorate a context manager"

        ```python
        from onetl.hooks import hook, HookPriority


        @hook
        class SimpleContextManager:
            def __init__(self, *args, **kwargs):
                ...

            def __enter__(self):
                ...
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                ...
                return False


        @hook(enabled=True, priority=HookPriority.FIRST)
        class ContextManagerWithProcessResult:
            def __init__(self, *args, **kwargs):
                ...

            def __enter__(self):
                ...
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                ...
                return False

            def process_result(self, result):
                # special method to handle method result call
                return modify(result)

            ...

        ```
    """

    def inner_wrapper(callback: Callable[P, T]) -> Callable[P, T]:
        if isinstance(callback, Hook):
            msg = "@hook decorator can be applied only once"
            raise TypeError(msg)

        result = Hook(callback=callback, enabled=enabled, priority=priority)
        return wraps(callback)(result)

    if inp is None:
        return inner_wrapper

    return inner_wrapper(inp)
