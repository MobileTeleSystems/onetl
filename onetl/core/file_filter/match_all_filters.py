import logging
from typing import Iterable

from onetl.base import BaseFileFilter
from onetl.impl import RemotePath, path_repr

log = logging.getLogger(__name__)


def match_all_filters(filters: Iterable[BaseFileFilter], path: RemotePath) -> bool:
    if not filters:
        return True

    not_match = []
    for file_filter in filters:
        if not file_filter.match(path):
            not_match.append(file_filter)

    if not_match:
        if len(not_match) > 1:
            filters_str = "filters " + ", ".join(repr(item) for item in not_match)
        else:
            filters_str = f"filter {not_match[0]!r}"

        log.debug(f"|FileFilter| Path {path_repr(path)} does NOT MATCH {filters_str}, skipping")
        return False

    log.debug(f"|FileFilter| Path {path_repr(path)} does match all filters")
    return True
