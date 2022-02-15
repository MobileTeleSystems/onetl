from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
from functools import wraps
from pathlib import PosixPath, Path
import os
from typing import Any, Callable, Generator
from logging import getLogger

from onetl.connection.connection_abc import ConnectionABC
from onetl.log import LOG_INDENT

log = getLogger(__name__)


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
        topdown: bool = True,
        onerror: Callable = None,
        exclude_dirs: list[str] = None,
    ) -> Generator[str, list[str], list[str]]:
        """
        Iterate over directory tree and return a tuple (dirpath,
        dirnames, filenames) on each iteration, like the `os.walk`
        function (see https://docs.python.org/library/os.html#os.walk ).
        """
        if not exclude_dirs:
            exclude_dirs = []
        try:
            items = self._listdir(top)
        except Exception as err:
            if onerror:
                onerror(err)
            return
        dirs, nondirs = [], []
        for item in items:
            name = self.get_name(item)
            full_name = PosixPath(top) / name
            if self.is_dir(top, item):
                if not self.excluded_dir(full_name, exclude_dirs):
                    dirs.append(name)
            else:
                nondirs.append(name)
        if topdown:
            yield top, dirs, nondirs
        for name in dirs:
            path = PosixPath(top) / name
            yield from self.walk(path, topdown, onerror, exclude_dirs)
        if not topdown:
            yield top, dirs, nondirs

    def rmdir(self, path: os.PathLike | str, recursive: bool = False) -> None:
        if recursive:
            for file in self._listdir(path):
                name = self.get_name(file)
                full_name = PosixPath(path) / name

                if self.is_dir(path, file):
                    self.rmdir(full_name, recursive=True)
                else:
                    self.remove_file(full_name)
            self.rmdir(path)
        else:
            self.client.rmdir(os.fspath(path))
            log.info(f"|{self.__class__.__name__}| Successfully removed directory {path}")

    def excluded_dir(self, full_name: os.PathLike | str, exclude_dirs: list[os.PathLike | str]) -> bool:
        for exclude_dir in exclude_dirs:
            if PosixPath(exclude_dir) == PosixPath(full_name):
                return True
        return False

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
