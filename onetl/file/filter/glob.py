# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import glob

try:
    from pydantic.v1 import validator
except (ImportError, AttributeError):
    from pydantic import validator  # type: ignore[no-redef, assignment]

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel


class Glob(BaseFileFilter, FrozenModel):
    """Filter files or directories with path matching a glob expression.

    .. versionadded:: 0.8.0
        Replaces deprecated ``onetl.core.FileFilter``

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
