from __future__ import annotations

import os
from enum import Enum
from abc import abstractmethod
from dataclasses import dataclass, field
from functools import wraps
from logging import getLogger
from pathlib import Path, PosixPath
from typing import Any, Callable, Iterator, TYPE_CHECKING
from pydantic import BaseModel

from onetl.connection.connection_abc import ConnectionABC
from onetl.log import LOG_INDENT

if TYPE_CHECKING:
    from onetl.core.file_filter.file_filter import BaseFileFilter

log = getLogger(__name__)


class WriteMode(Enum):
    ERROR = "error"
    IGNORE = "ignore"
    OVERWRITE = "overwrite"
    DELETE_ALL = "delete_all"


# Workaround for cached_property
def cached(f):
    @wraps(f)  # NOQA: WPS430
    def wrapped(self, *args, **kwargs):  # NOQA: WPS430
        key = f"{self.__class__.__name__}_{f.__name__}_{self.user}_{self.host}_cached_val"
        existing = getattr(wrapped, key, None)
        if existing is not None:
            return existing
        result = f(self, *args, **kwargs)
        setattr(wrapped, key, result)
        return result

    return wrapped


@dataclass(frozen=True)
class FileConnection(ConnectionABC):
    host: str
    user: str
    port: int
    password: str = field(repr=False, default="")

    class Options(BaseModel):  # noqa: WPS431
        """File write options"""

        mode: WriteMode = WriteMode.ERROR
        delete_source: bool = False

        class Config:  # noqa: WPS431
            allow_population_by_field_name = True
            frozen = True

    @abstractmethod
    def get_client(self) -> Any:
        """"""

    @property
    @cached
    def client(self):
        return self.get_client()

    def check(self):
        try:
            log.info(f"|{self.__class__.__name__}| Check connection availability...")
            log.info("|onETL| Using connection:")
            log.info(" " * LOG_INDENT + f"type = {self.__class__.__name__}")
            log.info(" " * LOG_INDENT + f"host = {self.host}")
            log.info(" " * LOG_INDENT + f"user = {self.user}")
            self.listdir("/")
            log.info(f"|{self.__class__.__name__}| Connection is available")
        except Exception as e:
            msg = f"Connection is unavailable:\n{e}"
            log.exception(f"|{self.__class__.__name__}| {msg}")
            raise RuntimeError(msg)

    def download_file(self, remote_file_path: os.PathLike | str, local_file_path: os.PathLike | str) -> None:
        self._download_file(remote_file_path, local_file_path)
        log.info(f"|Local FS| Successfully downloaded file: {local_file_path} ")

    def remove_file(self, remote_file_path: os.PathLike | str) -> None:
        if not self.path_exists(remote_file_path):
            log.info(f"|{self.__class__.__name__}| File {remote_file_path} does not exist, nothing to remove")
            return

        self._remove_file(remote_file_path)
        log.info(f"|{self.__class__.__name__}| Successfully removed file: {remote_file_path} ")

    def mkdir(self, path: os.PathLike | str) -> None:
        self._mkdir(path)
        log.info(f"|{self.__class__.__name__}| Successfully created directory: {path}")

    def upload_file(self, local_file_path: os.PathLike | str, remote_file_path: os.PathLike | str) -> None:
        self._upload_file(local_file_path, remote_file_path)
        log.info(f"|{self.__class__.__name__}| Successfully uploaded file: {remote_file_path}")

    def rename(self, source: os.PathLike | str, target: os.PathLike | str) -> None:
        self._rename(source, target)
        log.info(f"|{self.__class__.__name__}| Successfully renamed file {source} to {target}")

    @abstractmethod
    def is_dir(self, top, item) -> bool:
        """"""

    @abstractmethod
    def get_name(self, item) -> Path:
        """"""

    @abstractmethod
    def path_exists(self, path: os.PathLike | str) -> bool:
        """"""

    def listdir(self, path: os.PathLike | str) -> list[Path]:
        return [self.get_name(item) for item in self._listdir(path)]

    def walk(
        self,
        top: os.PathLike | str,
        filter: BaseFileFilter | None = None,  # noqa: WPS125
        onerror: Callable = None,
    ) -> Iterator[tuple[str, list[str], list[str]]] | None:
        """
        Iterate over directory tree and return a tuple (dirpath,
        dirnames, filenames) on each iteration, like the `os.walk`
        function (see https://docs.python.org/library/os.html#os.walk ).
        """

        try:
            listed_directories = self._listdir(top)
        except Exception as err:
            if onerror:
                onerror(err)
            return

        dirs, files = [], []
        for directory in listed_directories:
            name = self.get_name(directory)
            full_name = PosixPath(top) / name
            if self.is_dir(top, directory):
                log.info(f"|{self.__class__.__name__}| Checking directory: {full_name}")

                if not filter or filter.match_dir(full_name):
                    log.info(f"|{self.__class__.__name__}| Directory approved.")
                    dirs.append(name)

            else:
                log.info(f"|{self.__class__.__name__}| Checking file: {full_name}")

                if not filter or filter.match_file(full_name):
                    log.info(f"|{self.__class__.__name__}| File approved.")
                    files.append(name)

        for name in dirs:
            path = PosixPath(top) / name
            yield from self.walk(top=path, onerror=onerror, filter=filter)

        yield top, dirs, files  # root dir, included dirs, included files

    def rmdir(self, path: os.PathLike | str, recursive: bool = False) -> None:
        if not self.path_exists(path):
            log.info(f"|{self.__class__.__name__}| Directory {path} does not exist, nothing to remove")
            return

        if recursive:
            for file in self._listdir(path):
                name = self.get_name(file)
                full_name = PosixPath(path) / name

                if self.is_dir(path, file):
                    self.rmdir(full_name, recursive=True)
                else:
                    self.remove_file(full_name)

        self.client.rmdir(os.fspath(path))
        log.info(f"|{self.__class__.__name__}| Successfully removed directory {path}")

    @abstractmethod
    def _download_file(self, remote_file_path: os.PathLike | str, local_file_path: os.PathLike | str) -> None:
        """"""

    @abstractmethod
    def _remove_file(self, remote_file_path: os.PathLike | str) -> None:
        """"""

    @abstractmethod
    def _mkdir(self, path: os.PathLike | str) -> None:
        """"""

    @abstractmethod
    def _upload_file(self, local_file_path: os.PathLike | str, remote_file_path: os.PathLike | str) -> None:
        """"""

    @abstractmethod
    def _rename(self, source: os.PathLike | str, target: os.PathLike | str) -> None:
        """"""

    @abstractmethod
    def _listdir(self, path: os.PathLike) -> list:
        """"""
