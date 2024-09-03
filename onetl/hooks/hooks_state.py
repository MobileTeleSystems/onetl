# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from contextlib import contextmanager

from onetl.log import NOTICE

logger = logging.getLogger(__name__)


class HooksState:
    _enabled: bool = True

    @classmethod
    def enabled(cls) -> bool:
        return cls._enabled

    @classmethod
    def stop(cls) -> None:
        """
        Stop all hooks for all classes.

        .. versionadded:: 0.7.0

        Examples
        --------

        .. code:: python

            from onetl.hooks import stop_all_hooks

            # hooks are executed

            stop_all_hooks()

            # all hooks are stopped now
        """
        if cls._enabled:
            logger.log(NOTICE, "|Hooks| Stopping all hooks")
            cls._enabled = False
        else:
            logger.log(NOTICE, "|Hooks| Hooks already stopped")

    @classmethod
    def resume(cls) -> None:
        """
        Resume all onETL hooks.

        .. note::

            This function does not enable hooks which were disabled by :obj:`onetl.hooks.hook.Hook.disable`,
            or stopped by :obj:`onetl.hooks.support_hooks.suspend_hooks`.

        .. versionadded:: 0.7.0

        Examples
        --------

        .. code:: python

            from onetl.hooks import resume_all_hooks, stop_all_hooks

            stop_all_hooks()

            # hooks are stopped

            resume_all_hooks()

            # all hooks are executed now
        """
        if cls._enabled:
            logger.log(NOTICE, "|Hooks| Hooks are not stopped, nothing to resume")
        else:
            logger.log(NOTICE, "|Hooks| Resuming all hooks")
            cls._enabled = True

    @classmethod
    @contextmanager
    def skip(cls):
        """
        Temporary stop all onETL hooks. Designed to be used as context manager or decorator.

        .. note::

            If hooks were stopped by :obj:`~stop_all_hooks`, they will not be resumed
            after exiting the context/decorated function.
            You should call :obj:`~resume_all_hooks` explicitly.

        .. versionadded:: 0.7.0

        Examples
        --------

        .. tabs::

            .. code-tab:: py Context manager syntax

                from onetl.hooks import skip_all_hooks

                # hooks are enabled

                with skip_all_hooks():
                    # hooks are stopped here
                    ...

                # hook state is restored

            .. code-tab:: py Decorator syntax

                from onetl.hooks import skip_all_hooks

                # hooks are enabled


                @skip_all_hooks()
                def main():
                    # hooks are stopped here
                    ...


                main()
        """

        if not cls._enabled:
            logger.log(NOTICE, "|Hooks| All hooks are disabled, nothing to skip")
            yield
        else:
            logger.log(NOTICE, "|Hooks| Skipping all hooks...")
            cls._enabled = False

            try:
                yield
            finally:
                logger.log(NOTICE, "|Hooks| Restoring all hooks...")
                cls._enabled = True


skip_all_hooks = HooksState.skip
stop_all_hooks = HooksState.stop
resume_all_hooks = HooksState.resume
