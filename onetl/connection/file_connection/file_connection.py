from __future__ import annotations

import os
from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

from pydantic import BaseModel

from onetl.base import BaseFileConnection, BaseFileFilter, FileStatProtocol
from onetl.exception import (
    DirectoryNotEmptyError,
    DirectoryNotFoundError,
    NotAFileError,
)
from onetl.impl import RemoteDirectory, RemoteFile
from onetl.log import LOG_INDENT

log = getLogger(__name__)


class FileWriteMode(Enum):
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


@dataclass(frozen=True)  # noqa: WPS214
class FileConnection(BaseFileConnection):
    host: str
    user: str
    port: int
    password: str = field(repr=False, default="")

    class Options(BaseModel):  # noqa: WPS431
        """File write options"""

        mode: FileWriteMode = FileWriteMode.ERROR
        delete_source: bool = False

        class Config:  # noqa: WPS431
            allow_population_by_field_name = True
            frozen = True

    @property
    @cached
    def client(self):
        return self._get_client()

    def check(self) -> None:
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

    def is_file(self, path: os.PathLike | str) -> bool:
        if not self.path_exists(path):
            raise FileNotFoundError(f"|{self.__class__.__name__}| File '{path}' does not exist")

        return self._is_file(path)

    def is_dir(self, path: os.PathLike | str) -> bool:
        if not self.path_exists(path):
            raise DirectoryNotFoundError(f"|{self.__class__.__name__}| Directory '{path}' does not exist")

        return self._is_dir(path)

    def get_stat(self, path: os.PathLike | str) -> FileStatProtocol:
        if not self.is_file(path):
            raise NotAFileError(f"|{self.__class__.__name__}| '{path}' is not a file")

        return self._get_stat(path)

    def download_file(
        self,
        remote_file_path: os.PathLike | str,
        local_file_path: os.PathLike | str,
        replace: bool = True,
    ) -> Path:
        if not self.is_file(remote_file_path):
            raise NotAFileError(f"|{self.__class__.__name__}| '{remote_file_path}' is not a file")

        local_file = Path(local_file_path)
        if local_file.exists():
            if not local_file.is_file():
                raise NotAFileError(f"|LocalFS| '{local_file_path}' is not a file")

            error_msg = f"|LocalFS| '{local_file_path}' already exist"
            if not replace:
                raise FileExistsError(error_msg)

            log.warning(f"{error_msg}, replacing")
            local_file.unlink()

        local_file.parent.mkdir(parents=True, exist_ok=True)
        self._download_file(remote_file_path, local_file)
        log.info(f"|Local FS| Successfully downloaded file: '{local_file}'")
        return local_file

    def remove_file(self, remote_file_path: os.PathLike | str) -> None:
        if not self.path_exists(remote_file_path):
            log.warning(f"|{self.__class__.__name__}| File '{remote_file_path}' does not exist, nothing to remove")
            return

        if not self.is_file(remote_file_path):
            raise NotAFileError(f"|{self.__class__.__name__}| '{remote_file_path}' is not a file")

        self._remove_file(remote_file_path)
        log.info(f"|{self.__class__.__name__}| Successfully removed file: '{remote_file_path}'")

    def mkdir(self, path: os.PathLike | str) -> RemoteDirectory:
        if self.path_exists(path):
            if not self.is_dir(path):
                raise NotADirectoryError(f"|{self.__class__.__name__}| '{path}' is not a directory")

            log.info(f"|{self.__class__.__name__}| Directory '{path}' already exist")
            return RemoteDirectory(path=path)

        self._mkdir(path)
        log.info(f"|{self.__class__.__name__}| Successfully created directory: '{path}'")
        return RemoteDirectory(path=path)

    def upload_file(  # noqa: WPS238
        self,
        local_file_path: os.PathLike | str,
        remote_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> RemoteFile:
        local_file = Path(local_file_path)
        if not local_file.exists():
            raise FileNotFoundError(f"|LocalFS| File '{local_file_path}' does not exist")

        if not local_file.is_file():
            raise NotAFileError(f"|LocalFS| '{local_file_path}' is not a file")

        remote_file = PurePosixPath(remote_file_path)

        if self.path_exists(remote_file):
            if not self.is_file(remote_file):
                raise NotAFileError(f"|{self.__class__.__name__}| '{remote_file}' is not a file")

            error_msg = f"|{self.__class__.__name__}| File '{remote_file}' already exists"
            if not replace:
                raise FileExistsError(error_msg)

            log.warning(f"{error_msg}, removing")
            self._remove_file(remote_file)

        self._mkdir(remote_file.parent)
        self._upload_file(local_file, remote_file)
        stat = self.get_stat(remote_file)
        log.info(f"|{self.__class__.__name__}| Successfully uploaded file: '{remote_file}'")
        return RemoteFile(path=remote_file, stats=stat)

    def rename_file(
        self,
        source_file_path: os.PathLike | str,
        target_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> RemoteFile:
        if not self.is_file(source_file_path):
            raise NotAFileError(f"|{self.__class__.__name__}| '{source_file_path}' is not a file")

        target_file = PurePosixPath(target_file_path)

        if self.path_exists(target_file_path):
            if not self.is_file(target_file_path):
                raise NotAFileError(f"|{self.__class__.__name__}| '{target_file_path}' is not a file")

            error_msg = f"|{self.__class__.__name__}| File '{target_file_path}' already exists"
            if not replace:
                raise FileExistsError(error_msg)

            log.warning(f"{error_msg}, removing")
            self._remove_file(target_file_path)

        self._mkdir(target_file.parent)
        self._rename(source_file_path, target_file)
        log.info(f"|{self.__class__.__name__}| Successfully renamed file '{source_file_path}' to '{target_file}'")

        stat = self.get_stat(target_file)
        return RemoteFile(path=target_file, stats=stat)

    def listdir(self, path: os.PathLike | str) -> list[RemoteDirectory | RemoteFile]:
        result = []
        for item in self._listdir(path):
            name = self._get_item_name(item)

            if self._is_item_dir(path, item):
                result.append(RemoteDirectory(path=name))
            else:
                stat = self._get_item_stat(path, item)
                result.append(RemoteFile(path=name, stats=stat))

        return result

    def walk(
        self,
        top: os.PathLike | str,
        filter: BaseFileFilter | None = None,  # noqa: WPS125
    ) -> Iterator[tuple[RemoteDirectory, list[RemoteDirectory], list[RemoteFile]]]:
        """
        Iterate over directory tree and return a tuple (dirpath,
        dirnames, filenames) on each iteration, like the `os.walk`
        function (see https://docs.python.org/library/os.html#os.walk ).
        """

        root = RemoteDirectory(path=top)
        dirs, files = [], []

        for item in self._listdir(root):
            name = self._get_item_name(item)
            if self._is_item_dir(root, item):
                folder = RemoteDirectory(path=root / name)
                log.info(f"|{self.__class__.__name__}| Checking directory: '{folder}'")

                if filter and not filter.match(folder):
                    log.info(f"|{self.__class__.__name__}| Directory does not match the filter, skipping")
                else:
                    log.info(f"|{self.__class__.__name__}| Directory does match the filter")
                    dirs.append(RemoteDirectory(path=name))

            else:
                stat = self._get_item_stat(top, item)
                file = RemoteFile(path=root / name, stats=stat)
                log.info(f"|{self.__class__.__name__}| Checking file: '{file}'")

                if filter and not filter.match(file):
                    log.info(f"|{self.__class__.__name__}| File does not match the filter, skipping")
                else:
                    log.info(f"|{self.__class__.__name__}| File does match the filter")
                    files.append(RemoteFile(path=name, stats=stat))

        for name in dirs:
            path = root / name
            yield from self.walk(top=path, filter=filter)

        yield top, dirs, files

    def rmdir(self, path: os.PathLike | str, recursive: bool = False) -> None:
        if not self.path_exists(path):
            log.info(f"|{self.__class__.__name__}| Directory '{path}' does not exist, nothing to remove")
            return

        if not self.is_dir(path):
            raise NotADirectoryError(f"|{self.__class__.__name__}| '{path}' is not a directory")

        if not recursive and self._listdir(path):
            raise DirectoryNotEmptyError(f"Cannot delete non-empty directory: '{path}'")

        if recursive:
            self._rmdir_recursive(path)
        else:
            self._rmdir(path)

        log.info(f"|{self.__class__.__name__}| Successfully removed directory: '{path}'")

    def _rmdir_recursive(self, path: os.PathLike | str) -> None:
        for item in self._listdir(path):
            name = self._get_item_name(item)
            full_name = PurePosixPath(path) / name

            if self._is_item_dir(path, item):
                self._rmdir_recursive(full_name)
            else:
                self._remove_file(full_name)

        self._rmdir(path)

    def _get_item_name(self, item: str) -> str:
        return item

    def _is_item_dir(self, top: os.PathLike | str, item: str) -> bool:
        return self._is_dir(PurePosixPath(top) / self._get_item_name(item))

    def _is_item_file(self, top: os.PathLike | str, item: str) -> bool:
        return self._is_file(PurePosixPath(top) / self._get_item_name(item))

    def _get_item_stat(self, top: os.PathLike | str, item: str) -> FileStatProtocol:
        return self._get_stat(PurePosixPath(top) / self._get_item_name(item))

    @abstractmethod
    def _get_client(self) -> Any:
        """"""

    @abstractmethod
    def _download_file(self, remote_file_path: os.PathLike | str, local_file_path: os.PathLike | str) -> Path:
        """"""

    @abstractmethod
    def _get_stat(self, path: os.PathLike | str) -> FileStatProtocol:
        """"""

    @abstractmethod
    def _remove_file(self, remote_file_path: os.PathLike | str) -> None:
        """"""

    @abstractmethod
    def _mkdir(self, path: os.PathLike | str) -> None:
        """"""

    @abstractmethod
    def _upload_file(self, local_file_path: os.PathLike | str, remote_file_path: os.PathLike | str) -> RemoteFile:
        """"""

    @abstractmethod
    def _rename(self, source: os.PathLike | str, target: os.PathLike | str) -> None:
        """"""

    @abstractmethod
    def _listdir(self, path: os.PathLike | str) -> list:
        """"""

    @abstractmethod
    def _rmdir(self, path: os.PathLike | str) -> None:
        """"""
