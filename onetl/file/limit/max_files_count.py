# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging

try:
    from pydantic.v1 import validator
except (ImportError, AttributeError):
    from pydantic import validator  # type: ignore[no-redef, assignment]

from onetl.base import BaseFileLimit, PathProtocol
from onetl.impl import FrozenModel

log = logging.getLogger(__name__)


class MaxFilesCount(BaseFileLimit, FrozenModel):
    """Limits the total number of files handled by :ref:`file-downloader` or :ref:`file-mover`.

    All files until specified limit (including) will be downloaded/moved, but ``limit+1`` will not.

    This doesn't apply to directories.

    .. versionadded:: 0.8.0
        Replaces deprecated ``onetl.core.FileLimit``

    Parameters
    ----------

    limit : int

    Examples
    --------

    Create filter which allows to download/move up to 100 files, but stops on 101:

    .. code:: python

        from onetl.file.limit import MaxFilesCount

        limit = MaxFilesCount(100)
    """

    limit: int

    _handled: int = 0

    def __init__(self, limit: int):
        # this is only to allow passing glob as positional argument
        super().__init__(limit=limit)  # type: ignore

    def __repr__(self):
        return f"{self.__class__.__name__}({self.limit})"

    @validator("limit")
    def _limit_cannot_be_negative(cls, value):
        if value <= 0:
            raise ValueError("Limit should be positive number")
        return value

    def reset(self):
        self._handled = 0
        return self

    def stops_at(self, path: PathProtocol) -> bool:
        if self.is_reached:
            return True

        if path.is_dir():
            # directories count does not matter
            return False

        self._handled += 1
        return self.is_reached

    @property
    def is_reached(self) -> bool:
        return self._handled > self.limit
