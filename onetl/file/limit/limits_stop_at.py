# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import Iterable

from onetl.base import BaseFileLimit, PathProtocol

log = logging.getLogger(__name__)


def limits_stop_at(path: PathProtocol, limits: Iterable[BaseFileLimit]) -> bool:
    """
    Check if some of limits stops at given path.

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

    .. code:: python

        from onetl.file.limit import MaxFilesCount, limits_stop_at
        from onetl.impl import LocalPath

        limits = [MaxFilesCount(1)]

        assert not limits_stop_at(LocalPath("/path/to/file.csv"), limits)
        assert limits_stop_at(LocalPath("/path/to/file.csv"), limits)
    """
    reached = []
    for limit in limits:
        if limit.stops_at(path):
            reached.append(limit)

    if reached:
        log.debug("|FileLimit| Limits %r are reached", reached)
        return True

    return False
