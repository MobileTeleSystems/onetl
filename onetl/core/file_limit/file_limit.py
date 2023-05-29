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
import textwrap
import warnings

from pydantic import validator

from onetl.base import BaseFileLimit, PathProtocol
from onetl.impl import FrozenModel
from onetl.log import log_with_indent

log = logging.getLogger(__name__)


class FileLimit(BaseFileLimit, FrozenModel):
    """Limits the number of downloaded files.

    .. deprecated:: 0.8.0

        Use :obj:`MaxFilesCount <onetl.file.limit.max_files_count.MaxFilesCount>` instead.

    Parameters
    ----------

    count_limit : int, default = 100

        Number of downloaded files at a time.

    Examples
    --------

    Create a FileLimit object and set the amount in it:

    .. code:: python

        from onetl.core import FileLimit

        limit = FileLimit(count_limit=1500)

    If you create a :obj:`onetl.file.file_downloader.file_downloader.FileDownloader` object without
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
        return self._counter >= self.count_limit

    def log_options(self, indent: int = 0):
        for key, value in self.dict(by_alias=True).items():  # noqa: WPS528
            log_with_indent("%s = %r", key, value, indent=indent)

    @validator("count_limit")
    def log_deprecated(cls, value):
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
            stacklevel=3,  # 1 is current method, 2 is BaseModel internals, 3 is user code
        )
        return value
