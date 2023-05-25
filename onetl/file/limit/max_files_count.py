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

from onetl.base import BaseFileLimit, PathProtocol
from onetl.impl import FrozenModel

log = logging.getLogger(__name__)


class MaxFilesCount(BaseFileLimit, FrozenModel):
    """Limits the total number of files handled by :ref:`file-downloader` or :ref:`file-mover`.

    Parameters
    ----------

    limit : int

        All files until ``limit`` (including) will be downloaded/moved, but ``limit+1`` will not.

    Examples
    --------

    Create filter which allows to handle 100 files, but stops on 101:

    .. code:: python

        from onetl.file.limit import MaxFilesCount

        limit = MaxFilesCount(100)
    """

    limit: int

    _counter: int = 0

    def __init__(self, limit: int):
        # this is only to allow passing glob as positional argument
        super().__init__(limit=limit)  # type: ignore

    def __repr__(self):
        return f"{self.__class__.__name__}({self.limit})"

    def reset(self):
        self._counter = 0
        return self

    def stops_at(self, path: PathProtocol) -> bool:
        if self.is_reached:
            return True

        if path.is_dir():
            # directories count does not matter
            return False

        self._counter += 1
        return self.is_reached

    @property
    def is_reached(self) -> bool:
        return self._counter >= self.limit
