# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging

from onetl.base.path_protocol import PathWithStatsProtocol

try:
    from pydantic.v1 import ByteSize, validator
except (ImportError, AttributeError):
    from pydantic import ByteSize, validator  # type: ignore[no-redef, assignment]

from onetl.base import BaseFileLimit, PathProtocol
from onetl.impl import FrozenModel

log = logging.getLogger(__name__)


class TotalFilesSize(BaseFileLimit, FrozenModel):
    """Limits the total size of files handled by :ref:`file-downloader` or :ref:`file-mover`.

    Sum of downloaded/moved files should be less or equal to specified size. After that all files with non-zero size will be ignored.

    This doesn't apply to directories or files with no size information,

    .. versionadded:: 0.13.0

    ..note::

        SI unit prefixes means that ``1KB`` == ``1 kilobyte`` == ``1000 bytes``.
        If you need ``1024 bytes``, use ``1 KiB`` == ``1 kibibyte``.

    Parameters
    ----------

    limit : int or str

    Examples
    --------

    Create filter which allows to download/move files with total size up to 1GiB, but not higher:

    .. code:: python

        from onetl.file.limit import MaxFilesCount

        limit = TotalFilesSize("1GiB")
    """

    limit: ByteSize

    _handled: int = 0

    def __init__(self, limit: int | str):
        # this is only to allow passing glob as positional argument
        super().__init__(limit=limit)  # type: ignore

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.limit.human_readable()}")'

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

        if not path.is_file():
            # directories count does not matter
            return False

        if not isinstance(path, PathWithStatsProtocol):
            return False

        self._handled += path.stat().st_size
        return self.is_reached

    @property
    def is_reached(self) -> bool:
        return self._handled >= self.limit
