#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
