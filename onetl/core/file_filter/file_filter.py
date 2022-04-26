from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import PurePath
from dataclasses import dataclass, field
from logging import getLogger

log = getLogger(__name__)


class BaseFileFilter(ABC):
    @abstractmethod
    def match_dir(self, path: PurePath) -> bool:
        pass  # noqa: WPS420

    @abstractmethod
    def match_file(self, path: PurePath) -> bool:
        pass  # noqa: WPS420


@dataclass
class FileFilter(BaseFileFilter):
    """Class needed to determine HOW files are loaded.

    Parameters
    ----------

    glob: str | None
        pattern according to which only files matching it will be taken

    exclude_dirs: list
        list of directories files from which will not be included in the download


    Examples
    --------

    Create exclude_dir filter:

    ..code::

        downloader = FileDownloader(
                    connection=file_connection,
                    source_path=source_path,
                    local_path=local_path,
                    filter=FileFilter(exclude_dirs=["/export/news_parse/exclude_dir"]),
                )

    Create glob filter:

    ..code::

        downloader = FileDownloader(
                    connection=file_connection,
                    source_path=source_path,
                    local_path=local_path,
                    filter=FileFilter(glob="*.csv"),
                )

    """

    glob: str | None = None
    exclude_dirs: list = field(default_factory=list)

    def match_dir(self, path: PurePath) -> bool:
        if self.exclude_dirs:
            for exclude_dir in self.exclude_dirs:
                if PurePath(exclude_dir) in path.parents or PurePath(exclude_dir) == path:
                    return False  # Exclude directory

        return True

    def match_file(self, path: PurePath):
        if not self.glob:
            return True

        return path.match(self.glob)

    def log_options(self):
        indent = len(f"|{self.__class__.__name__}| ") + 2

        log.info(" " * indent + "filter:\n")
        for option in self.__dict__:  # noqa: WPS528
            log.info(" " * indent + f"{option} = {self.__dict__[option]}")
