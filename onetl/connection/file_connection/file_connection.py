from __future__ import annotations

import os
from abc import abstractmethod
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Iterator

from pydantic import BaseModel

from onetl.base import BaseFileConnection, BaseFileFilter, FileStatProtocol
from onetl.exception import (
    DirectoryNotEmptyError,
    DirectoryNotFoundError,
    NotAFileError,
)
from onetl.impl import FileWriteMode, LocalPath, RemoteDirectory, RemoteFile, RemotePath
from onetl.log import LOG_INDENT

log = getLogger(__name__)


@dataclass(frozen=True)  # noqa: WPS214
class FileConnection(BaseFileConnection):
    host: str
    user: str
    port: int
    password: str = field(repr=False, default="")

    _client: Any = field(init=False, repr=False, default=None)

    class Options(BaseModel):  # noqa: WPS431
        """File write options"""

        mode: FileWriteMode = FileWriteMode.ERROR
        delete_source: bool = False

        class Config:  # noqa: WPS431
            allow_population_by_field_name = True
            frozen = True

    @property
    def client(self):
        if self._client and not self._is_client_closed():
            return self._client

        client = self._get_client()
        object.__setattr__(self, "_client", client)  # noqa: WPS609
        return client

    def close(self):
        """
        Close all connections, opened by other methods call.

        Examples
        --------

        Get directory content and close connection:

        .. code:: python

            content = connection.listdir("/mydir")
            assert content
            connection.close()

            # or

            with connection:
                content = connection.listdir("/mydir")
                content = connection.listdir("/mydir/abc")

        """

        if self._client:
            self._close_client()

        object.__setattr__(self, "_client", None)  # noqa: WPS609

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def check(self):
        try:
            log.info(f"|{self.__class__.__name__}| Check connection availability...")
            log.info("|onETL| Using connection:")
            log.info(LOG_INDENT + f"type = {self.__class__.__name__}")
            log.info(LOG_INDENT + f"host = {self.host}")
            log.info(LOG_INDENT + f"user = {self.user}")
            self.listdir("/")
            log.info(f"|{self.__class__.__name__}| Connection is available")
        except Exception as e:
            msg = f"Connection is unavailable:\n{e}"
            log.exception(f"|{self.__class__.__name__}| {msg}")
            raise RuntimeError(msg)

        return self

    def is_file(self, path: os.PathLike | str) -> bool:
        remote_path = RemotePath(path)

        if not self.path_exists(remote_path):
            raise FileNotFoundError(f"|{self.__class__.__name__}| File '{remote_path}' does not exist")

        return self._is_file(remote_path)

    def is_dir(self, path: os.PathLike | str) -> bool:
        remote_path = RemotePath(path)

        if not self.path_exists(remote_path):
            raise DirectoryNotFoundError(f"|{self.__class__.__name__}| Directory '{remote_path}' does not exist")

        return self._is_dir(remote_path)

    def get_stat(self, path: os.PathLike | str) -> FileStatProtocol:
        remote_path = RemotePath(path)

        if not self.is_file(remote_path):
            raise NotAFileError(f"|{self.__class__.__name__}| '{remote_path}' is not a file")

        return self._get_stat(remote_path)

    def get_file(self, path: os.PathLike | str) -> RemoteFile:
        remote_path = RemotePath(path)
        stat = self.get_stat(remote_path)

        return RemoteFile(path=remote_path, stats=stat)

    def read_text(self, path: os.PathLike | str, encoding: str = "utf-8", **kwargs) -> str:
        remote_path = RemotePath(path)

        if not self.is_file(remote_path):
            raise NotAFileError(f"|{self.__class__.__name__}| '{remote_path}' is not a file")

        return self._read_text(remote_path, encoding=encoding, **kwargs)

    def read_bytes(self, path: os.PathLike | str, **kwargs) -> bytes:
        remote_path = RemotePath(path)

        if not self.is_file(remote_path):
            raise NotAFileError(f"|{self.__class__.__name__}| '{remote_path}' is not a file")

        return self._read_bytes(remote_path, **kwargs)

    def write_text(self, path: os.PathLike | str, content: str, encoding: str = "utf-8", **kwargs) -> RemoteFile:
        remote_path = RemotePath(path)
        self.mkdir(remote_path.parent)
        self._write_text(remote_path, content=content, encoding=encoding, **kwargs)

        return self.get_file(remote_path)

    def write_bytes(self, path: os.PathLike | str, content: bytes, **kwargs) -> RemoteFile:
        remote_path = RemotePath(path)
        self.mkdir(remote_path.parent)
        self._write_bytes(remote_path, content=content, **kwargs)

        return self.get_file(remote_path)

    def download_file(
        self,
        remote_file_path: os.PathLike | str,
        local_file_path: os.PathLike | str,
        replace: bool = True,
    ) -> LocalPath:
        remote_file = RemotePath(remote_file_path)
        if not self.is_file(remote_file):
            raise NotAFileError(f"|{self.__class__.__name__}| '{remote_file}' is not a file")

        local_file = LocalPath(local_file_path)
        if local_file.exists():
            if not local_file.is_file():
                raise NotAFileError(f"|LocalFS| '{remote_file}' is not a file")

            error_msg = f"|LocalFS| '{local_file}' already exist"
            if not replace:
                raise FileExistsError(error_msg)

            log.warning(f"{error_msg}, replacing")
            local_file.unlink()

        local_file.parent.mkdir(parents=True, exist_ok=True)
        self._download_file(remote_file_path, local_file)
        log.info(f"|Local FS| Successfully downloaded file: '{local_file}'")
        return local_file

    def remove_file(self, remote_file_path: os.PathLike | str) -> None:
        remote_file = RemotePath(remote_file_path)

        if not self.path_exists(remote_file):
            log.warning(f"|{self.__class__.__name__}| File '{remote_file}' does not exist, nothing to remove")
            return

        if not self.is_file(remote_file):
            raise NotAFileError(f"|{self.__class__.__name__}| '{remote_file}' is not a file")

        self._remove_file(remote_file)
        log.info(f"|{self.__class__.__name__}| Successfully removed file: '{remote_file}'")

    def mkdir(self, path: os.PathLike | str) -> RemoteDirectory:
        remote_directory = RemotePath(path)

        if self.path_exists(remote_directory):
            if not self.is_dir(remote_directory):
                raise NotADirectoryError(f"|{self.__class__.__name__}| '{remote_directory}' is not a directory")

            return RemoteDirectory(path=remote_directory)

        self._mkdir(remote_directory)
        log.info(f"|{self.__class__.__name__}| Successfully created directory: '{remote_directory}'")
        return RemoteDirectory(path=remote_directory)

    def upload_file(  # noqa: WPS238
        self,
        local_file_path: os.PathLike | str,
        remote_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> RemoteFile:
        local_file = LocalPath(local_file_path)
        if not local_file.exists():
            raise FileNotFoundError(f"|LocalFS| File '{local_file_path}' does not exist")

        if not local_file.is_file():
            raise NotAFileError(f"|LocalFS| '{local_file_path}' is not a file")

        remote_file = RemotePath(remote_file_path)
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
        result = self.get_file(remote_file)
        log.info(f"|{self.__class__.__name__}| Successfully uploaded file: '{remote_file}'")
        return result

    def rename_file(
        self,
        source_file_path: os.PathLike | str,
        target_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> RemoteFile:
        source_file = RemotePath(source_file_path)
        if not self.is_file(source_file):
            raise NotAFileError(f"|{self.__class__.__name__}| '{source_file}' is not a file")

        target_file = RemotePath(target_file_path)
        if self.path_exists(target_file):
            if not self.is_file(target_file):
                raise NotAFileError(f"|{self.__class__.__name__}| '{target_file}' is not a file")

            error_msg = f"|{self.__class__.__name__}| File '{target_file}' already exists"
            if not replace:
                raise FileExistsError(error_msg)

            log.warning(f"{error_msg}, removing")
            self._remove_file(target_file)

        self._mkdir(target_file.parent)
        self._rename(source_file, target_file)
        log.info(f"|{self.__class__.__name__}| Successfully renamed file '{source_file}' to '{target_file}'")

        return self.get_file(target_file)

    def listdir(self, path: os.PathLike | str) -> list[RemoteDirectory | RemoteFile]:
        remote_directory = RemotePath(path)

        if not self.is_dir(path):
            raise NotADirectoryError(f"|{self.__class__.__name__}| '{remote_directory}' is not a directory")

        result = []
        for item in self._listdir(remote_directory):
            name = self._get_item_name(item)

            if self._is_item_dir(remote_directory, item):
                result.append(RemoteDirectory(path=name))
            else:
                stat = self._get_item_stat(remote_directory, item)
                result.append(RemoteFile(path=name, stats=stat))

        return result

    def walk(  # noqa: WPS231
        self,
        top: os.PathLike | str,
        filter: BaseFileFilter | None = None,  # noqa: WPS125
    ) -> Iterator[tuple[RemoteDirectory, list[RemoteDirectory], list[RemoteFile]]]:
        """
        Iterate over directory tree and return a tuple (dirpath,
        dirnames, filenames) on each iteration, like the `os.walk`
        function (see https://docs.python.org/library/os.html#os.walk ).
        """

        remote_directory = RemotePath(top)
        if not self.is_dir(remote_directory):
            raise NotADirectoryError(f"|{self.__class__.__name__}| '{remote_directory}' is not a directory")

        root = RemoteDirectory(path=remote_directory)
        dirs, files = [], []

        for item in self._listdir(root):
            name = self._get_item_name(item)
            if self._is_item_dir(root, item):
                folder = RemoteDirectory(path=root / name)

                if filter and not filter.match(folder):
                    log.debug(
                        f"|{self.__class__.__name__}| Directory '{os.fspath(folder)}' "
                        "does NOT MATCH the filter, skipping",
                    )
                else:
                    log.debug(f"|{self.__class__.__name__}| Directory '{os.fspath(folder)}' is matching the filter")
                    dirs.append(RemoteDirectory(path=name))

            else:
                stat = self._get_item_stat(remote_directory, item)
                file = RemoteFile(path=root / name, stats=stat)

                if filter and not filter.match(file):
                    log.debug(
                        f"|{self.__class__.__name__}| File '{os.fspath(file)}' does NOT MATCH the filter, skipping",
                    )
                else:
                    log.debug(f"|{self.__class__.__name__}| File '{os.fspath(file)}' is matching the filter")
                    files.append(RemoteFile(path=name, stats=stat))

        # if a nested directory was encountered, then the same method is called recursively
        for name in dirs:
            path = root / name
            yield from self.walk(top=path, filter=filter)

        yield top, dirs, files

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def rmdir(self, path: os.PathLike | str, recursive: bool = False) -> None:
        remote_directory = RemotePath(path)

        if not self.path_exists(remote_directory):
            return

        if not self.is_dir(remote_directory):
            raise NotADirectoryError(f"|{self.__class__.__name__}| '{remote_directory}' is not a directory")

        if not recursive and self._listdir(remote_directory):
            raise DirectoryNotEmptyError(f"Cannot delete non-empty directory: '{remote_directory}'")

        if recursive:
            self._rmdir_recursive(remote_directory)
        else:
            self._rmdir(remote_directory)

        log.info(f"|{self.__class__.__name__}| Successfully removed directory: '{remote_directory}'")

    def _rmdir_recursive(self, root: RemotePath) -> None:
        for item in self._listdir(root):
            name = self._get_item_name(item)
            path = root / name

            if self._is_item_dir(path, item):
                self._rmdir_recursive(path)
            else:
                self._remove_file(path)

        self._rmdir(root)

    def _get_item_name(self, item) -> str:
        return item

    def _is_item_dir(self, top: RemotePath, item) -> bool:
        return self._is_dir(top / self._get_item_name(item))

    def _is_item_file(self, top: RemotePath, item) -> bool:
        return self._is_file(top / self._get_item_name(item))

    def _get_item_stat(self, top: RemotePath, item) -> FileStatProtocol:
        return self._get_stat(top / self._get_item_name(item))

    @abstractmethod
    def _get_client(self) -> Any:
        """"""

    @abstractmethod
    def _is_client_closed(self) -> bool:
        """"""

    @abstractmethod
    def _close_client(self) -> None:
        """"""

    @abstractmethod
    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> LocalPath:
        """"""

    @abstractmethod
    def _get_stat(self, path: RemotePath) -> FileStatProtocol:
        """"""

    @abstractmethod
    def _remove_file(self, remote_file_path: RemotePath) -> None:
        """"""

    @abstractmethod
    def _mkdir(self, path: RemotePath) -> None:
        """"""

    @abstractmethod
    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> RemoteFile:
        """"""

    @abstractmethod
    def _rename(self, source: RemotePath, target: RemotePath) -> None:
        """"""

    @abstractmethod
    def _listdir(self, path: RemotePath) -> list:
        """"""

    @abstractmethod
    def _rmdir(self, path: RemotePath) -> None:
        """"""

    @abstractmethod
    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        """"""

    @abstractmethod
    def _read_bytes(self, path: RemotePath, **kwargs) -> bytes:
        """"""

    @abstractmethod
    def _write_text(self, path: RemotePath, content: str, encoding: str, **kwargs) -> None:
        """"""

    @abstractmethod
    def _write_bytes(self, path: RemotePath, content: bytes, **kwargs) -> None:
        """"""
