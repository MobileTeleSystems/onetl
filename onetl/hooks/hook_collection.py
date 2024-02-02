# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterable

from onetl.hooks.hook import Hook
from onetl.log import NOTICE

logger = logging.getLogger(__name__)


class HookCollection:
    """
    Representation of hooks collection.

    Examples
    --------

    .. code:: python

        from onetl.hooks.hook import Hook
        from onetl.hooks.hook_collection import HookCollection

        hooks = HookCollection([Hook(callback=func1), Hook(callback=func2), ...])
    """

    def __init__(self, hooks: list[Hook] | HookCollection | None = None):
        self._hooks: list[Hook] = list(hooks) if hooks else []
        self._enabled: bool = True

    @property
    def active(self):
        """
        Return HookCollection but containing only hooks with enabled state.

        If called after :obj:`~stop` or inside :obj:`~skip`, empty collection will be returned.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import Hook
            from onetl.hooks.hook_collection import HookCollection

            hooks = HookCollection(
                [
                    Hook(callback=func1, enabled=True),
                    Hook(callback=func2, enabled=False),
                ]
            )

            assert hooks.active == HookCollection([Hook(callback=func1)])
        """

        if self._enabled:
            return self.__class__(hooks=[hook for hook in self._hooks if hook.enabled])

        return self.__class__()

    def stop(self) -> None:
        """
        Stop all hooks in the collection.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import Hook
            from onetl.hooks.hook_collection import HookCollection

            hooks = HookCollection(
                [
                    Hook(callback=func1),
                    Hook(callback=func2),
                ]
            )

            hooks.stop()
            assert hooks.active == HookCollection()
        """
        self._enabled = False

    def resume(self) -> None:
        """
        Resume all hooks in the collection.

        .. note::

            If hook is disabled by :obj:`onetl.hooks.hook.Hook.disable`, it will stay disabled.
            You should call :obj:`onetl.hooks.hook.Hook.enable` explicitly.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import Hook
            from onetl.hooks.hook_collection import HookCollection

            hooks = HookCollection(
                [
                    Hook(callback=func1),
                    Hook(callback=func2),
                ]
            )

            hooks.resume()

            assert hooks.active == HookCollection(
                [
                    Hook(callback=func1),
                    Hook(callback=func2),
                ]
            )
        """

        self._enabled = True

    @contextmanager
    def skip(self):
        """
        Context manager which temporary stops all the hooks in the collection.

        .. note::

            If hooks were stopped by :obj:`~stop`, they will not be resumed
            after exiting the context/decorated function.
            You should call :obj:`~resume` explicitly.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import Hook
            from onetl.hooks.hook_collection import HookCollection

            hooks = HookCollection(
                [
                    Hook(callback=func1),
                    Hook(callback=func2),
                ]
            )

            # hooks state is same as created by constructor

            with hooks.skip():
                # all hooks are disabled here
                ...

            # hooks state is restored as it was before entering the context manager
        """

        if not self._enabled:
            logger.log(NOTICE, "|Hooks| Hooks already disabled, nothing to skip")
            yield
        else:
            logger.log(NOTICE, "|Hooks| Disabling hooks")
            self._enabled = False
            yield
            self._enabled = True

    def add(self, item: Hook):
        """Appends hook to the collection.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import Hook
            from onetl.hooks.hook_collection import HookCollection

            hooks = HookCollection(
                [
                    Hook(callback=func1, enabled=True),
                ]
            )

            new_hook = Hook(callback=func2, enabled=False)
            hooks.add(new_hook)

            assert hooks == HookCollection(
                [
                    Hook(callback=func1, enabled=True),
                    Hook(callback=func2, enabled=False),
                ]
            )
        """
        self._hooks.append(item)

    def extend(self, hooks: Iterable[Hook]):
        """Extends collection using a iterator.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import Hook
            from onetl.hooks.hook_collection import HookCollection

            hooks = HookCollection(
                [
                    Hook(callback=func1, enabled=True),
                ]
            )

            new_hooks = [Hook(callback=func2, enabled=False)]
            hooks.extend(new_hook)

            assert hooks == HookCollection(
                [
                    Hook(callback=func1, enabled=True),
                    Hook(callback=func2, enabled=False),
                ]
            )
        """
        self._hooks.extend(hooks)

    def __iter__(self):
        """Iterate over hooks in the collection.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import Hook
            from onetl.hooks.hook_collection import HookCollection

            hooks = HookCollection(
                [
                    Hook(callback=func1, enabled=True),
                    Hook(callback=func2, enabled=True),
                ]
            )

            for hook in hooks:
                assert hook.enabled
        """
        return iter(self._hooks)

    def __len__(self):
        """Return collection length.

        Examples
        --------

        .. code:: python

            from onetl.hooks.hook import Hook
            from onetl.hooks.hook_collection import HookCollection

            hooks = HookCollection(
                [
                    Hook(callback=func1, enabled=True),
                    Hook(callback=func2, enabled=True),
                ]
            )

            assert len(hooks) == 2
        """
        return len(self._hooks)
