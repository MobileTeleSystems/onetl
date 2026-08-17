# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import textwrap
import warnings

from pydantic import field_validator
from typing_extensions import deprecated

from onetl.base import BaseFileLimit, PathProtocol
from onetl.impl import FrozenModel


@deprecated("Deprecated in 0.8.0 and will be removed in 1.0.0. Use MaxFilesCount instead", category=None)
class FileLimit(BaseFileLimit, FrozenModel):
    """Limits the number of downloaded files.

    !!! warning "Deprecated since 0.8.0"

        Use [MaxFilesCount][onetl.file.limit.max_files_count.MaxFilesCount] instead.

    Parameters
    ----------

    count_limit

        Number of downloaded files at a time.

    Examples
    --------

    Create a FileLimit object and set the amount in it:

    ```python
    from onetl.core import FileLimit

    limit = FileLimit(count_limit=1500)
    ```
    If you create a [onetl.file.file_downloader.file_downloader.FileDownloader][] object without
    specifying the limit option, it will download with a limit of 100 files.
    """

    count_limit: int = 100

    _counter: int = 0

    def reset(self):
        self._counter = 0
        return self

    def stops_at(self, path: PathProtocol) -> bool:
        if self.is_reached:
            return True

        if path.is_dir():
            return False

        # directories count does not matter
        self._counter += 1
        return self.is_reached

    @property
    def is_reached(self) -> bool:
        return self._counter > self.count_limit

    @field_validator("count_limit", mode="before")
    @classmethod
    def _deprecated(cls, value):
        message = f"""
            Using FileLimit is deprecated since v0.8.0 and will be removed in v1.0.0.

            Please replace:
                from onetl.core import FileLimit

                limit=FileLimit(count_limit={value})

            With:
                from onetl.file.limit import MaxFilesCount

                limits=[MaxFilesCount({value})]
        """

        warnings.warn(
            textwrap.dedent(message).strip(),
            category=UserWarning,
            stacklevel=3,
        )
        return value
