# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from onetl.base import BaseFileLimit

log = logging.getLogger(__name__)


def reset_limits(limits: "Iterable[BaseFileLimit]") -> "list[BaseFileLimit]":
    """
    Reset limits state.

    Parameters
    ----------
    limits
        Limits to reset.

    Returns
    -------
    list[onetl.base.base_file_limit.BaseFileLimit]
        List with limits, but with reset state.

        List may contain original filters with reset state, or new copies.
        This is an implementation detail of [reset][onetl.base.base_file_limit.BaseFileLimit.reset] method.

    Examples
    --------

    ```python
    >>> from onetl.file.limit import MaxFilesCount, limits_reached, limits_stop_at, reset_limits
    >>> from onetl.impl import LocalPath
    >>> limits = [MaxFilesCount(1)]
    >>> limits_reached(limits)
    False
    >>> # do something
    >>> limits_stop_at(LocalPath("/path/to/file1.csv"), limits)
    False
    >>> limits_stop_at(LocalPath("/path/to/file2.csv"), limits)
    True
    >>> limits_reached(limits)
    True
    >>> new_limits = reset_limits(limits)
    >>> limits_reached(new_limits)
    False

    ```
    """
    return [limit.reset() for limit in limits]
