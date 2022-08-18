from __future__ import annotations

import logging
from typing import Iterable

from onetl.base import BaseFileLimit
from onetl.impl import RemotePath

log = logging.getLogger(__name__)


def limits_reached(limits: Iterable[BaseFileLimit], path: RemotePath) -> bool:
    if not limits:
        return False

    reached = []
    for limit in limits:
        if limit.stops_at(path):
            reached.append(limit)

    if reached:
        if len(reached) > 1:
            limits_str = "|FileLimit| Limits " + ", ".join(repr(item) for item in reached) + " are reached"
        else:
            limits_str = f"|FileLimit| Limit {reached[0]!r} is reached"

        log.debug(limits_str)
        return True

    return False
