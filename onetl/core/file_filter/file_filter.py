# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import glob
import os
import re
import textwrap
import warnings
from typing import List, Optional, Union

from typing_extensions import deprecated

try:
    from pydantic.v1 import Field, root_validator, validator
except (ImportError, AttributeError):
    from pydantic import Field, root_validator, validator  # type: ignore[no-redef, assignment]

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel, RemotePath


@deprecated("Deprecated in 0.8.0 and will be removed in 1.0.0. Use Glob, Regexp or ExcludeDir instead", category=None)
class FileFilter(BaseFileFilter, FrozenModel):
    r"""Filter files or directories by their path.

    .. deprecated:: 0.8.0

        Use :obj:`Glob <onetl.file.filter.glob.Glob>`, :obj:`Regexp <onetl.file.filter.regexp.Regexp>`
        or :obj:`ExcludeDir <onetl.file.filter.exclude_dir.ExcludeDir>` instead.

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

        from onetl.core import FileFilter

        file_filter = FileFilter(exclude_dirs=["/export/news_parse/exclude_dir"])

    Create glob filter:

    .. code:: python

        from onetl.core import FileFilter

        file_filter = FileFilter(glob="*.csv")

    Create regexp filter:

    .. code:: python

        from onetl.core import FileFilter

        file_filter = FileFilter(regexp=r"\d+\.csv")

        # or

        import re

        file_filter = FileFilter(regexp=re.compile("\d+\.csv"))

    Not allowed:

    .. code:: python

        from onetl.core import FileFilter

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

    @root_validator
    def log_deprecated(cls, value: dict) -> dict:
        imports = []
        old_filters = []
        new_filters = []
        glob = value.get("glob")  # noqa: WPS442
        if glob is not None:
            imports.append("Glob")
            old_filters.append(f"glob={glob!r}")
            new_filters.append(f"Glob({glob!r})")

        regexp = value.get("regexp")
        if regexp is not None:
            imports.append("Regexp")
            old_filters.append(f"regexp={regexp.pattern!r}")
            new_filters.append(f"Regexp({regexp.pattern!r})")

        exclude_dirs = value.get("exclude_dirs")
        if exclude_dirs:
            imports.append("ExcludeDir")
            exclude_dirs_str = [repr(os.fspath(exclude_dir)) for exclude_dir in exclude_dirs]
            old_filters.append(f"exclude_dirs=[{', '.join(exclude_dirs_str)}]")
            new_filters.extend(f"ExcludeDir({item})" for item in exclude_dirs_str)

        if not imports:
            return value

        message = f"""
            Using FileFilter is deprecated since v0.8.0 and will be removed in v1.0.0.

            Please replace:
                from onetl.core import FileFilter

                filter=FileFilter({', '.join(old_filters)})

            With:
                from onetl.file.filter import {', '.join(imports)}

                filters=[{', '.join(new_filters)}]
        """

        warnings.warn(
            textwrap.dedent(message).strip(),
            category=UserWarning,
            stacklevel=3,  # 1 is current method, 2 is BaseModel internals, 3 is user code
        )
        return value

    def match(self, path: PathProtocol) -> bool:
        """False means it does not match the template by which you want to receive files"""

        if self.exclude_dirs:
            for exclude_dir in self.exclude_dirs:
                if path.is_dir() and exclude_dir == path:
                    return False
                if exclude_dir in path.parents:
                    return False

        if self.glob and path.is_file():
            return path.match(self.glob)

        if self.regexp and path.is_file():
            return self.regexp.search(os.fspath(path)) is not None

        return True
