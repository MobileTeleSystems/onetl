# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os

from pydantic import field_validator

from onetl.base import BaseFileFilter, PathProtocol, PurePathProtocol
from onetl.impl import FrozenModel, RemotePath


class ExcludeDir(BaseFileFilter, FrozenModel):
    """Filter files or directories which are included in a specific directory.

    !!! success "Added in 0.8.0"
        Replaces deprecated `onetl.core.FileFilter`

    Parameters
    ----------

    path

        Path to directory which should be excluded.

    Examples
    --------

    Create exclude dir filter:

    ```python
    from onetl.file.filter import ExcludeDir

    exclude_dir = ExcludeDir("/export/news_parse/exclude_dir")

    ```
    """

    path: PurePathProtocol

    def __init__(self, path: str | os.PathLike):
        # this is only to allow passing glob as positional argument
        super().__init__(path=path)  # type: ignore[call-arg]

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.path}')"

    def match(self, path: PathProtocol) -> bool:
        if path.is_dir() and self.path == path:
            return False

        return self.path not in path.parents

    @field_validator("path", mode="before")
    @classmethod
    def _validate_path(cls, value: str | os.PathLike) -> PurePathProtocol:
        if isinstance(value, PurePathProtocol):
            return value
        return RemotePath(os.fspath(value))
