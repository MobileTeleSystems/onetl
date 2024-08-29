# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Iterable

from onetl.base import BaseFileFilter
from onetl.base.path_protocol import PathProtocol
from onetl.impl import path_repr

log = logging.getLogger(__name__)


def match_all_filters(path: PathProtocol, filters: Iterable[BaseFileFilter]) -> bool:
    """
    Check if input path satisfies all the filters.

    .. versionadded:: 0.8.0

    Parameters
    ----------
    path : :obj:`onetl.base.path_protocol.PathProtocol`
        Path to check.

    filters : Iterable of :obj:`onetl.base.base_file_filter.BaseFileFilter`
        Filters to test path against.

    Returns
    -------
    ``True`` if path matches all the filters, ``False`` otherwise.

    If filters are empty, returns ``True``.

    Examples
    --------

    >>> from onetl.file.filter import Glob, ExcludeDir, match_all_filters
    >>> from onetl.impl import RemoteFile, RemotePathStat
    >>> filters = [Glob("*.csv"), ExcludeDir("/excluded")]
    >>> match_all_filters(RemoteFile("/path/to/file.csv", stats=RemotePathStat()), filters)
    True
    >>> match_all_filters(RemoteFile("/path/to/file.txt", stats=RemotePathStat()), filters)
    False
    >>> match_all_filters(RemoteFile("/excluded/path/file.csv", stats=RemotePathStat()), filters)
    False
    """

    empty = True
    not_match = []
    for file_filter in filters:
        empty = False
        if not file_filter.match(path):
            not_match.append(file_filter)

    if empty:
        return True

    if not_match:
        log.debug(
            "|FileFilter| Path %s does NOT MATCH filters %r, skipping",
            path_repr(path),
            not_match,
        )
        return False

    log.debug("|FileFilter| Path %s does match all filters", path_repr(path))
    return True
