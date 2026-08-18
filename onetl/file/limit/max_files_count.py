# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging

from pydantic import PositiveInt

from onetl.base import BaseFileLimit, PathProtocol
from onetl.impl import FrozenModel

log = logging.getLogger(__name__)


class MaxFilesCount(BaseFileLimit, FrozenModel):
    """Limits the total number of files handled by [onetl.file.file_downloader.file_downloader.FileDownloader][] or [onetl.file.file_mover.file_mover.FileMover][].

    All files until specified limit (including) will be downloaded/moved, but `limit+1` will not.

    This doesn't apply to directories.

    !!! success "Added in 0.8.0"
        Replaces deprecated `onetl.core.FileLimit`

    Parameters
    ----------

    limit
        Maximum number of files to be handled.

    Examples
    --------

    Create filter which allows to download/move up to 100 files, but stops on 101:

    ```python
    from onetl.file.limit import MaxFilesCount

    limit = MaxFilesCount(100)

    ```
    """

    limit: PositiveInt

    _handled: int = 0

    def __init__(self, limit: int):
        # this is only to allow passing glob as positional argument
        super().__init__(limit=limit)  # type: ignore[call-arg]

    def __repr__(self):
        return f"{self.__class__.__name__}({self.limit})"

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
