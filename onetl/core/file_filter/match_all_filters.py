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
        log.debug(
            "|FileFilter| Path %s does NOT MATCH filters %r, skipping",
            path_repr(path),
            not_match,
        )
        return False

    log.debug("|FileFilter| Path %s does match all filters", path_repr(path))
    return True
