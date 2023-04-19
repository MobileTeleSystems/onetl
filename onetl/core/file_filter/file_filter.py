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

import glob
import os
import re
from typing import List, Optional, Union

from pydantic import Field, root_validator, validator

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel, RemotePath
from onetl.log import log_with_indent


class FileFilter(BaseFileFilter, FrozenModel):
    r"""Filter files or directories by their path.

    Parameters
    ----------

    glob : str | None, default ``None``

        Pattern (e.g. ``*.csv``) for which any **file** (only file) path should match

        .. warning::

            Mutually exclusive with ``regexp``

    regexp : str | re.Pattern | None, default ``None``

        Regular expression (e.g. ``\d+\.csv``) for which any **file** (only file) path should match.

        If input is a string, regular expression will be compiles using ``re.IGNORECASE`` and ``re.DOTALL`` flags

        .. warning::

            Mutually exclusive with ``glob``

    exclude_dirs : list[os.PathLike | str], default ``[]``

        List of directories which should not be a part of a file or directory path


    Examples
    --------

    Create exclude_dir filter:

    .. code:: python

        file_filter = FileFilter(exclude_dirs=["/export/news_parse/exclude_dir"])

    Create glob filter:

    .. code:: python

        file_filter = FileFilter(glob="*.csv")

    Create regexp filter:

    .. code:: python

        file_filter = FileFilter(regexp=r"\d+\.csv")

        # or

        import re

        file_filter = FileFilter(regexp=re.compile("\d+\.csv"))

    Not allowed:

    .. code:: python

        FileFilter()  # will raise ValueError, at least one argument should be passed
    """

    class Config:
        arbitrary_types_allowed = True

    glob: Optional[str] = None
    regexp: Optional[re.Pattern] = None
    exclude_dirs: List[RemotePath] = Field(default_factory=list)

    @validator("glob", pre=True)
    def check_glob(cls, value: str) -> str:
        if not glob.has_magic(value):
            raise ValueError("Invalid glob")

        return value

    @validator("regexp", pre=True)
    def check_regexp(cls, value: Union[re.Pattern, str]) -> re.Pattern:
        if isinstance(value, str):
            return re.compile(value, re.IGNORECASE | re.DOTALL)

        return value

    @validator("exclude_dirs", each_item=True, pre=True)
    def check_exclude_dir(cls, value: Union[str, os.PathLike]) -> RemotePath:
        return RemotePath(value)

    @root_validator
    def disallow_empty_fields(cls, value: dict) -> dict:
        if value.get("glob") is None and value.get("regexp") is None and not value.get("exclude_dirs"):
            raise ValueError("One of the following fields must be set: `glob`, `regexp`, `exclude_dirs`")

        return value

    @root_validator
    def disallow_both_glob_and_regexp(cls, value: dict) -> dict:
        if value.get("glob") and value.get("regexp"):
            raise ValueError("Only one of `glob`, `regexp` fields can passed, not both")

        return value

    def match(self, path: PathProtocol) -> bool:
        """False means it does not match the template by which you want to receive files"""

        if self.exclude_dirs:
            path = path if path.is_dir() else path.parent
            for exclude_dir in self.exclude_dirs:
                if exclude_dir in path.parents or exclude_dir == path:
                    return False

        if self.glob and path.is_file():
            return path.match(self.glob)

        if self.regexp and path.is_file():
            return self.regexp.search(os.fspath(path)) is not None

        return True

    def log_options(self, indent: int = 0):
        for key, value in self.dict(exclude_none=True, by_alias=True).items():  # noqa: WPS528
            log_with_indent("%s = %r", key, value, indent=indent)
