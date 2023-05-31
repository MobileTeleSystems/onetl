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

from pydantic import validator

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel


class Glob(BaseFileFilter, FrozenModel):
    """Filter files or directories with path matching a glob expression.

    Parameters
    ----------

    pattern : str

        Pattern (e.g. ``*.csv``) for which any **file** (only file) path should match

    Examples
    --------

    Create glob filter:

    .. code:: python

        from onetl.file.filter import Glob

        glob = Glob("*.csv")
    """

    class Config:
        arbitrary_types_allowed = True

    pattern: str

    def __init__(self, pattern: str):
        # this is only to allow passing glob as positional argument
        super().__init__(pattern=pattern)  # type: ignore

    def __repr__(self):
        return f"{self.__class__.__name__}({self.pattern!r})"

    def match(self, path: PathProtocol) -> bool:
        if not path.is_file():
            return True

        return path.match(self.pattern)

    @validator("pattern", pre=True)
    def _validate_pattern(cls, value: str) -> str:
        if not glob.has_magic(value):
            raise ValueError(f"Invalid glob: {value!r}")

        return value
