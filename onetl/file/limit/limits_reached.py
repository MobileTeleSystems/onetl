# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import Iterable

from onetl.base import BaseFileLimit

log = logging.getLogger(__name__)


def limits_reached(limits: Iterable[BaseFileLimit]) -> bool:
    """
    Check if any of limits reached.

    Parameters
    ----------
    limits : Iterable of :obj:`onetl.base.base_file_limit.BaseFileLimit`
        Limits to test.

    Returns
    -------
    ``True`` if any of limits is reached, ``False`` otherwise.

    If no limits are passed, returns ``False``.

    Examples
    --------

    .. code:: python

        from onetl.file.limit import MaxFilesCount, limits_reached, limits_stop_at
        from onetl.impl import LocalPath

        limits = [MaxFilesCount(2)]
        assert not limits_reached(limits)

        assert not limits_stop_at(LocalPath("/path/to/file.csv"), limits)
        assert limits_stop_at(LocalPath("/path/to/file.csv"), limits)

        assert limits_reached(limits)
    """
    debug = log.isEnabledFor(logging.DEBUG)

    reached = []
    for limit in limits:
        if limit.is_reached:
            if not debug:
                # fast path
                return True

            reached.append(limit)

    if reached:
        log.debug("|FileLimit| Limits %r are reached", reached)
        return True

    return False
