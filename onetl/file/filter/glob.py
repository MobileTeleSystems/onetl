# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import glob

from pydantic import field_validator

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel


class Glob(BaseFileFilter, FrozenModel):
    """Filter files or directories with path matching a glob expression.

    !!! success "Added in 0.8.0"
        Replaces deprecated `onetl.core.FileFilter`

    Parameters
    ----------

    pattern : str

        Pattern (e.g. `*.csv`) for which any **file** (only file) path should match

    Examples
    --------

    Create glob filter:

    ```python
    from onetl.file.filter import Glob

    glob = Glob("*.csv")

    ```
    """

    pattern: str

    def __init__(self, pattern: str):
        # this is only to allow passing glob as positional argument
        super().__init__(pattern=pattern)  # type: ignore[call-arg]

    def __repr__(self):
        return f"{self.__class__.__name__}({self.pattern!r})"

    def match(self, path: PathProtocol) -> bool:
        if not path.is_file():
            return True

        return path.match(self.pattern)

    @field_validator("pattern", mode="before")
    @classmethod
    def _validate_pattern(cls, value: str) -> str:
        if not glob.has_magic(value):
            msg = f"Invalid glob: {value!r}"
            raise ValueError(msg)
        return value
