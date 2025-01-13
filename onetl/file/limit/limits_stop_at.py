# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import Iterable

from onetl.base import BaseFileLimit, PathProtocol

log = logging.getLogger(__name__)


def limits_stop_at(path: PathProtocol, limits: Iterable[BaseFileLimit]) -> bool:
    """
    Check if some of limits stops at given path.

    .. versionadded:: 0.8.0

    Parameters
    ----------
    path : :obj:`onetl.base.path_protocol.PathProtocol`
        Path to check.

    limits : Iterable of :obj:`onetl.base.base_file_limit.BaseFileLimit`
        Limits to test path against.

    Returns
    -------
    ``True`` if any of limit is reached while handling the path, ``False`` otherwise.

    If no limits are passed, returns ``False``.

    Examples
    --------

    >>> from onetl.file.limit import MaxFilesCount, limits_stop_at
    >>> from onetl.impl import LocalPath
    >>> limits = [MaxFilesCount(2)]
    >>> limits_stop_at(LocalPath("/path/to/file1.csv"), limits)
    False
    >>> limits_stop_at(LocalPath("/path/to/file2.csv"), limits)
    False
    >>> limits_stop_at(LocalPath("/path/to/file3.csv"), limits)
    True
    """
    reached = []
    for limit in limits:
        if limit.stops_at(path):
            reached.append(limit)

    if reached:
        log.debug("|FileLimit| Limits %r are reached", reached)
        return True

    return False
