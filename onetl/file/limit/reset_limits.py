# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import Iterable

from onetl.base import BaseFileLimit

log = logging.getLogger(__name__)


def reset_limits(limits: Iterable[BaseFileLimit]) -> list[BaseFileLimit]:
    """
    Reset limits state.

    Parameters
    ----------
    limits : Iterable of :obj:`onetl.base.base_file_limit.BaseFileLimit`
        Limits to reset.

    Returns
    -------
    List with limits, but with reset state.

    List may contain original filters with reset state, or new copies.
    This is an implementation detail of :obj:`reset <onetl.base.base_file_limit.BaseFileLimit.reset>` method.

    Examples
    --------

    .. code:: python

        from onetl.file.limit import MaxFilesCount, limits_reached, reset_limits
        from onetl.impl import LocalPath

        limits = [MaxFilesCount(1)]

        assert not limits_reached(limits)
        # do something
        assert limits_reached(limits)

        new_limits = reset_limits(limits)
        assert not limits_reached(new_limits)
    """
    return [limit.reset() for limit in limits]
