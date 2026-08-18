# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Annotated

import annotated_types
from pydantic import ByteSize

from onetl.base import BaseFileLimit, PathProtocol
from onetl.base.path_protocol import PathWithStatsProtocol
from onetl.impl import FrozenModel

log = logging.getLogger(__name__)


class TotalFilesSize(BaseFileLimit, FrozenModel):
    """Limits the total size of files handled by [onetl.file.file_downloader.file_downloader.FileDownloader][] or [onetl.file.file_mover.file_mover.FileMover][].

    Calculates the sum of downloaded/moved files size (`.stat().st_size`),
    and checks that this sum is less or equal to specified limit.

    After limit is reached, no more files will be downloaded/moved.

    Doesn't affect directories, paths without `.stat()` method or files with zero size.

    !!! success "Added in 0.13.0"

    !!! note

        [SI unit prefixes](https://en.wikipedia.org/wiki/Byte#Multiple-byte_units)
        means that `1KB` == `1 kilobyte` == `1000 bytes`.
        If you need `1024 bytes`, use `1 KiB` == `1 kibibyte`.

    Parameters
    ----------

    limit
        Maximum total size of files to be handled. Can be an integer (bytes) or a string like `1GiB`.

    Examples
    --------

    Create filter which allows to download/move files with total size up to 1GiB, but not higher:

    ```python
    from onetl.file.limit import MaxFilesCount

    limit = TotalFilesSize("1GiB")

    ```
    """

    limit: Annotated[ByteSize, annotated_types.Gt(0)]

    _handled: int = 0

    def __init__(self, limit: int | str):
        # this is only to allow passing glob as positional argument
        super().__init__(limit=limit)  # type: ignore[call-arg]

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.limit.human_readable()}")'

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
        return self._handled > self.limit
