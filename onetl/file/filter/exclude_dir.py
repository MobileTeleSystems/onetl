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

import os

from pydantic import validator

from onetl.base import BaseFileFilter, PathProtocol, PurePathProtocol
from onetl.impl import FrozenModel, RemotePath


class ExcludeDir(BaseFileFilter, FrozenModel):
    """Filter files or directories which are included in a specific directory.

    Parameters
    ----------

    path : str or :obj:`os.PathLike`

        Path to directory which should be excluded.

    Examples
    --------

    Create exclude dir filter:

    .. code:: python

        from onetl.file.filter import ExcludeDir

        exclude_dir = ExcludeDir("/export/news_parse/exclude_dir")
    """

    class Config:
        arbitrary_types_allowed = True

    path: PurePathProtocol

    def __init__(self, path: str | os.PathLike):
        # this is only to allow passing glob as positional argument
        super().__init__(path=path)  # type: ignore

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.path}')"

    def match(self, path: PathProtocol) -> bool:
        if path.is_dir() and self.path == path:
            return False

        return self.path not in path.parents

    @validator("path", pre=True)
    def _validate_path(cls, value: str | os.PathLike) -> PurePathProtocol:
        if isinstance(value, PurePathProtocol):
            return value

        return RemotePath(os.fspath(value))
