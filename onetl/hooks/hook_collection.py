# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING

from onetl.log import NOTICE

if TYPE_CHECKING:
    from collections.abc import Iterable

    from onetl.hooks.hook import Hook

logger = logging.getLogger(__name__)


class HookCollection:
    """
    Representation of hooks collection.

    !!! success "Added in 0.7.0"

    Examples
    --------

    ```python
    from onetl.hooks.hook import Hook
    from onetl.hooks.hook_collection import HookCollection

    hooks = HookCollection([Hook(callback=func1), Hook(callback=func2), ...])

    ```
    """

    def __init__(self, hooks: "list[Hook] | HookCollection | None" = None):
        self._hooks: list[Hook] = list(hooks) if hooks else []
        self._enabled: bool = True

    @property
    def active(self):
        """
        Return new HookCollection but containing only hooks with `enabled=True` state.

        If called after [stop][] or inside [skip][], empty collection will be returned.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> from onetl.hooks.hook import Hook
        >>> from onetl.hooks.hook_collection import HookCollection
        >>> def func1(): ...
        >>> def func2(): ...
        >>> hooks = HookCollection(
        ...     [
        ...         Hook(callback=func1, enabled=True),
        ...         Hook(callback=func2, enabled=False),
        ...     ],
        ... )
        >>> len(hooks.active)
        1

        ```
        """

        if self._enabled:
            return self.__class__(hooks=[hook for hook in self._hooks if hook.enabled])

        return self.__class__()

    def stop(self) -> None:
        """
        Stop all hooks in the collection.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> from onetl.hooks.hook import Hook
        >>> from onetl.hooks.hook_collection import HookCollection
        >>> def func1(): ...
        >>> def func2(): ...
        >>> hooks = HookCollection(
        ...     [
        ...         Hook(callback=func1),
        ...         Hook(callback=func2),
        ...     ],
        ... )
        >>> hooks.stop()
        >>> hooks.active
        HookCollection([])

        ```
        """
        self._enabled = False

    def resume(self) -> None:
        """
        Resume all hooks in the collection.

        !!! note

            If hook is disabled by [onetl.hooks.hook.Hook.disable][], it will stay disabled.
            You should call [onetl.hooks.hook.Hook.enable][] explicitly.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> from onetl.hooks.hook import Hook
        >>> from onetl.hooks.hook_collection import HookCollection
        >>> def func1(): ...
        >>> def func2(): ...
        >>> hooks = HookCollection(
        ...     [
        ...         Hook(callback=func1),
        ...         Hook(callback=func2),
        ...     ],
        ... )
        >>> hooks.resume()
        >>> hooks.active # doctest: +SKIP
        HookCollection(
            [
                Hook(callback=func1),
                Hook(callback=func2),
            ]
        )

        ```
        """

        self._enabled = True

    @contextmanager
    def skip(self):
        """
        Context manager which temporary stops all the hooks in the collection.

        !!! note

            If hooks were stopped by [stop][], they will not be resumed
            after exiting the context/decorated function.
            You should call [resume][] explicitly.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> from onetl.hooks.hook import Hook
        >>> from onetl.hooks.hook_collection import HookCollection
        >>> def func1(): ...
        >>> def func2(): ...
        >>> hooks = HookCollection(
        ...     [
        ...         Hook(callback=func1),
        ...         Hook(callback=func2),
        ...     ],
        ... )
        >>> # hooks state is same as created by constructor
        >>> len(hooks.active)
        2
        >>> with hooks.skip():
        ...     # all hooks are disabled here
        ...     print(len(hooks.active))
        0
        >>> # hooks state is restored as it was before entering the context manager
        >>> len(hooks.active)
        2

        ```
        """

        if not self._enabled:
            logger.log(NOTICE, "|Hooks| Hooks already disabled, nothing to skip")
            yield
        else:
            logger.log(NOTICE, "|Hooks| Disabling hooks")
            self._enabled = False
            yield
            self._enabled = True

    def add(self, item: "Hook"):
        """Appends hook to the collection.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> from onetl.hooks.hook import Hook
        >>> from onetl.hooks.hook_collection import HookCollection
        >>> def func1(): ...
        >>> def func2(): ...
        >>> hooks = HookCollection(
        ...     [
        ...         Hook(callback=func1),
        ...     ],
        ... )

        ```

        ```python
        >>> new_hook = Hook(callback=func2)
        >>> hooks.add(new_hook)
        >>> len(hooks.active)
        2

        ```
        """
        self._hooks.append(item)

    def extend(self, hooks: "Iterable[Hook]"):
        """Extends collection using a iterator.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> from onetl.hooks.hook import Hook
        >>> from onetl.hooks.hook_collection import HookCollection
        >>> def func1(): ...
        >>> def func2(): ...
        >>> hooks = HookCollection(
        ...     [
        ...         Hook(callback=func1),
        ...     ],
        ... )

        ```

        ```python
        >>> new_hooks = [Hook(callback=func2)]
        >>> hooks.extend(new_hooks)
        >>> len(hooks.active)
        2

        ```
        """
        self._hooks.extend(hooks)

    def __iter__(self):
        """Iterate over hooks in the collection.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> from onetl.hooks.hook import Hook
        >>> from onetl.hooks.hook_collection import HookCollection
        >>> def func1(): ...
        >>> def func2(): ...
        >>> hooks = HookCollection(
        ...    [
        ...        Hook(callback=func1, enabled=True),
        ...        Hook(callback=func2, enabled=False),
        ...    ],
        ... )
        >>> for hook in hooks:
        ...    print(hook.enabled)
        True
        False

        ```
        """
        return iter(self._hooks)

    def __len__(self):
        """Return collection length.

        !!! success "Added in 0.7.0"

        Examples
        --------

        ```python
        >>> from onetl.hooks.hook import Hook
        >>> from onetl.hooks.hook_collection import HookCollection
        >>> def func1(): ...
        >>> def func2(): ...
        >>> hooks = HookCollection(
        ...     [
        ...         Hook(callback=func1),
        ...         Hook(callback=func2),
        ...     ],
        ... )
        >>> len(hooks)
        2

        ```
        """
        return len(self._hooks)

    def __repr__(self):
        return f"HookCollection({self._hooks})"
