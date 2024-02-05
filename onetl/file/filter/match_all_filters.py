# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
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

    .. code:: python

        from onetl.file.filter import Glob, ExcludeDir, match_all_filters
        from onetl.impl import LocalPath

        filters = [Glob("*.csv"), ExcludeDir("/excluded")]

        assert match_all_filters(LocalPath("/path/to/file.csv"), filters)
        assert not match_all_filters(LocalPath("/path/to/file.txt"), filters)
        assert not match_all_filters(LocalPath("/excluded/path/file.csv"), filters)
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
