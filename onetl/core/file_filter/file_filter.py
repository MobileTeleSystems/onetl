from __future__ import annotations

import os
from logging import getLogger
from pathlib import PurePosixPath
from typing import List, Optional, Union

from pydantic import BaseModel, Field, validator

from onetl.base import BaseFileFilter, PathProtocol

log = getLogger(__name__)


class FileFilter(BaseFileFilter, BaseModel):
    """Class needed to determine WHICH files are loaded.

    Parameters
    ----------

    glob : str | None, default ``None``

        Pattern according to which only files matching it will be taken

    exclude_dirs : list[os.PathLike | str], default ``[]``

        List of directories files from which will not be included in the download


    Examples
    --------

    Create exclude_dir filter:

    .. code:: python

        file_filter = FileFilter(exclude_dirs=["/export/news_parse/exclude_dir"])

    Create glob filter:

    .. code:: python

        file_filter = FileFilter(glob="*.csv")
    """

    class Config:  # noqa: WPS431
        arbitrary_types_allowed = True

    glob: Optional[str] = None
    exclude_dirs: List[PurePosixPath] = Field(default_factory=list)

    @validator("exclude_dirs", each_item=True, pre=True)
    def check_exclude_dir(cls, value: Union[str, os.PathLike]) -> PurePosixPath:  # noqa: N805
        return PurePosixPath(value)

    def match(self, path: PathProtocol) -> bool:
        if self.exclude_dirs and path.is_dir():
            for exclude_dir in self.exclude_dirs:
                if exclude_dir in path.parents or exclude_dir == path:
                    return False

        if self.glob and path.is_file():
            return path.match(self.glob)

        return True

    def log_options(self):
        indent = len(f"|{self.__class__.__name__}| ") + 2

        log.info(" " * indent + "filter:\n")
        for option in self.__dict__:  # noqa: WPS528
            log.info(" " * indent + f"{option} = {self.__dict__[option]}")
