# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import re

try:
    from pydantic.v1 import validator
except (ImportError, AttributeError):
    from pydantic import validator  # type: ignore[no-redef, assignment]

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel


class Regexp(BaseFileFilter, FrozenModel):
    r"""Filter files or directories with path matching a regular expression.

    .. versionadded:: 0.8.0
        Replaces deprecated ``onetl.core.FileFilter``

    Parameters
    ----------

    pattern : :obj:`re.Pattern`

        Regular expression (e.g. ``\d+\.csv``) for which any **file** (only file) path should match.

        If input is a string, regular expression will be compiles using ``re.IGNORECASE`` and ``re.DOTALL`` flags.

    Examples
    --------

    Create regexp filter from string:

    .. code:: python

        from onetl.file.filter import Regexp

        regexp = Regexp(r"\d+\.csv")

    Create regexp filter from :obj:`re.Pattern`:

    .. code:: python

        import re

        from onetl.file.filter import Regexp

        regexp = Regexp(re.compile(r"\d+\.csv", re.IGNORECASE | re.DOTALL))
    """

    class Config:
        arbitrary_types_allowed = True

    pattern: re.Pattern

    def __init__(self, pattern: str):
        # this is only to allow passing regexp as positional argument
        super().__init__(pattern=pattern)  # type: ignore

    def __repr__(self):
        return f"{self.__class__.__name__}({self.pattern!r})"

    def match(self, path: PathProtocol) -> bool:
        if not path.is_file():
            return True

        return self.pattern.search(os.fspath(path)) is not None

    @validator("pattern", pre=True)
    def _validate_pattern(cls, value: re.Pattern | str) -> re.Pattern:
        if isinstance(value, str):
            try:
                return re.compile(value, re.IGNORECASE | re.DOTALL)
            except re.error as e:
                raise ValueError(f"Invalid regexp: {value!r}") from e

        return value
