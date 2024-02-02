# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
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
