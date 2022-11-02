#  Copyright 2022 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import os
from abc import abstractmethod
from logging import getLogger
from typing import Any, Iterable, Iterator

from humanize import naturalsize

from onetl.base import (
    BaseFileConnection,
    BaseFileFilter,
    BaseFileLimit,
    PathStatProtocol,
)
from onetl.core.file_filter import match_all_filters
from onetl.core.file_limit import limits_reached
from onetl.exception import (
    DirectoryNotEmptyError,
    DirectoryNotFoundError,
    NotAFileError,
)
from onetl.impl import (
    FrozenModel,
    LocalPath,
    RemoteDirectory,
    RemoteFile,
    RemotePath,
    path_repr,
)
from onetl.log import log_with_indent

log = getLogger(__name__)


class FileConnection(BaseFileConnection, FrozenModel):  # noqa: WPS214
    _client: Any = None

    @property
    def client(self):
        if self._client and not self._is_client_closed():
            return self._client

        client = self._get_client()
        self._client = client  # noqa: WPS601
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

        self._client = None  # noqa: WPS601

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def check(self):
        log.info(f"|{self.__class__.__name__}| Checking connection availability...")
        self._log_parameters()

        try:
            self.listdir("/")
            log.info(f"|{self.__class__.__name__}| Connection is available")
        except Exception as e:
            log.exception(f"|{self.__class__.__name__}| Connection is unavailable")
            raise RuntimeError("Connection is unavailable") from e

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

    def get_stat(self, path: os.PathLike | str) -> PathStatProtocol:
        remote_path = RemotePath(path)

        return self._get_stat(remote_path)

    def get_directory(self, path: os.PathLike | str) -> RemoteDirectory:
        is_dir = self.is_dir(path)
        stat = self.get_stat(path)

        if not is_dir:
            raise NotADirectoryError(
                f"|{self.__class__.__name__}| {path_repr(RemoteFile(path, stats=stat))} is not a directory",
            )

        return RemoteDirectory(path=path, stats=stat)

    def get_file(self, path: os.PathLike | str) -> RemoteFile:
        is_file = self.is_file(path)
        stat = self.get_stat(path)
        remote_path = RemoteFile(path=path, stats=stat)

        if not is_file:
            raise NotAFileError(f"|{self.__class__.__name__}| {path_repr(remote_path)} is not a file")

        return remote_path

    def read_text(self, path: os.PathLike | str, encoding: str = "utf-8", **kwargs) -> str:
        log.debug(
            f"|{self.__class__.__name__}| Reading string with encoding '{encoding}' "
            f"and options {kwargs!r} from '{path}'",
        )

        remote_path = self.get_file(path)
        return self._read_text(remote_path, encoding=encoding, **kwargs)

    def read_bytes(self, path: os.PathLike | str, **kwargs) -> bytes:
        log.debug(f"|{self.__class__.__name__}| Reading bytes with options {kwargs!r} from '{path}'")

        remote_path = self.get_file(path)
        return self._read_bytes(remote_path, **kwargs)

    def write_text(self, path: os.PathLike | str, content: str, encoding: str = "utf-8", **kwargs) -> RemoteFile:
        if not isinstance(content, str):
            raise TypeError(f"content must be 'str', not '{content.__class__.__name__}'")

        log.debug(
            f"|{self.__class__.__name__}| Writing string size {len(content)} "
            f"with encoding '{encoding}' and options {kwargs!r} to '{path}'",
        )

        remote_path = RemotePath(path)
        self.mkdir(remote_path.parent)

        if self.path_exists(remote_path):
            file = self.get_file(remote_path)
            log.warning(
                f"|{self.__class__.__name__}| File {path_repr(file)} already exists and will be overwritten",
            )

        self._write_text(remote_path, content=content, encoding=encoding, **kwargs)

        return self.get_file(remote_path)

    def write_bytes(self, path: os.PathLike | str, content: bytes, **kwargs) -> RemoteFile:
        log.debug(
            f"|{self.__class__.__name__}| Writing {naturalsize(len(content))} with options {kwargs!r} to '{path}'",
        )

        remote_path = RemotePath(path)
        self.mkdir(remote_path.parent)

        if self.path_exists(remote_path):
            file = self.get_file(remote_path)
            log.warning(
                f"|{self.__class__.__name__}| File {path_repr(file)} already exists and will be overwritten",
            )

        self._write_bytes(remote_path, content=content, **kwargs)

        return self.get_file(remote_path)

    def download_file(
        self,
        remote_file_path: os.PathLike | str,
        local_file_path: os.PathLike | str,
        replace: bool = True,
    ) -> LocalPath:
        log.debug(f"|{self.__class__.__name__}| Downloading file '{remote_file_path}' to local '{local_file_path}'")

        remote_file = self.get_file(remote_file_path)
        local_file = LocalPath(local_file_path)

        if local_file.exists():
            if not local_file.is_file():
                raise NotAFileError(f"|LocalFS| {path_repr(local_file)} is not a file")

            error_msg = f"|LocalFS| File {path_repr(local_file)} already exists"
            if not replace:
                raise FileExistsError(error_msg)

            log.warning(f"{error_msg}, overwriting")
            local_file.unlink()

        log.debug(f"|Local FS| Creating target directory '{local_file.parent}'")
        local_file.parent.mkdir(parents=True, exist_ok=True)
        self._download_file(remote_file, local_file)

        if local_file.stat().st_size != remote_file.stat().st_size:
            raise RuntimeError(
                f"|{self.__class__.__name__}|"
                " The size of the downloaded file does not match the size of the file on the source",
            )

        log.info(f"|Local FS| Successfully downloaded file '{local_file}'")
        return local_file

    def remove_file(self, remote_file_path: os.PathLike | str) -> None:
        log.debug(f"|{self.__class__.__name__}| Removing file '{remote_file_path}'")

        if not self.path_exists(remote_file_path):
            log.debug(f"|{self.__class__.__name__}| File '{remote_file_path}' does not exist, nothing to remove")
            return

        file = self.get_file(remote_file_path)
        log.debug(f"|{self.__class__.__name__}| File to remove: {path_repr(file)}")

        self._remove_file(file)
        log.info(f"|{self.__class__.__name__}| Successfully removed file '{file}'")

    def mkdir(self, path: os.PathLike | str) -> RemoteDirectory:
        log.debug(f"|{self.__class__.__name__}| Creating directory '{path}'")
        remote_directory = RemotePath(path)

        if self.path_exists(remote_directory):
            return self.get_directory(remote_directory)

        self._mkdir(remote_directory)
        log.info(f"|{self.__class__.__name__}| Successfully created directory '{remote_directory}'")
        return self.get_directory(remote_directory)

    def upload_file(  # noqa: WPS238
        self,
        local_file_path: os.PathLike | str,
        remote_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> RemoteFile:
        log.debug(f"|{self.__class__.__name__}| Uploading local file '{local_file_path}' to '{remote_file_path}'")

        local_file = LocalPath(local_file_path)
        if not local_file.exists():
            raise FileNotFoundError(f"|LocalFS| File '{local_file}' does not exist")

        if not local_file.is_file():
            raise NotAFileError(f"|LocalFS| {path_repr(local_file)} is not a file")

        remote_file = RemotePath(remote_file_path)
        if self.path_exists(remote_file):
            file = self.get_file(remote_file_path)
            error_msg = f"|{self.__class__.__name__}| File {path_repr(file)} already exists"
            if not replace:
                raise FileExistsError(error_msg)

            log.warning(f"{error_msg}, overwriting")
            self._remove_file(remote_file)

        self.mkdir(remote_file.parent)

        self._upload_file(local_file, remote_file)
        result = self.get_file(remote_file)

        if result.stat().st_size != local_file.stat().st_size:
            raise RuntimeError(
                f"|{self.__class__.__name__}|"
                " The size of the uploaded file does not match the size of the file on the source",
            )

        log.info(f"|{self.__class__.__name__}| Successfully uploaded file '{remote_file}'")
        return result

    def rename_file(
        self,
        source_file_path: os.PathLike | str,
        target_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> RemoteFile:
        log.debug(f"|{self.__class__.__name__}| Renaming file '{source_file_path}' to '{target_file_path}'")

        source_file = self.get_file(source_file_path)
        target_file = RemotePath(target_file_path)

        if self.path_exists(target_file):
            file = self.get_file(target_file)
            error_msg = f"|{self.__class__.__name__}| File {path_repr(file)} already exists"
            if not replace:
                raise FileExistsError(error_msg)

            log.warning(f"{error_msg}, overwriting")
            self._remove_file(target_file)

        self.mkdir(target_file.parent)
        self._rename(source_file, target_file)
        log.info(f"|{self.__class__.__name__}| Successfully renamed file '{source_file}' to '{target_file}'")

        return self.get_file(target_file)

    def listdir(
        self,
        directory: os.PathLike | str,
        filters: Iterable[BaseFileFilter] | None = None,
        limits: Iterable[BaseFileLimit] | None = None,
    ) -> list[RemoteDirectory | RemoteFile]:
        log.debug(f"|{self.__class__.__name__}| Listing directory '{directory}'")

        filters = filters or []
        limits = limits or []

        for limit in limits:
            limit.reset()

        remote_directory = self.get_directory(directory)
        result = []

        for item in self._listdir(remote_directory):
            name = self._get_item_name(item)
            stat = self._get_item_stat(remote_directory, item)

            if self._is_item_dir(remote_directory, item):
                path = RemoteDirectory(path=name, stats=stat)
            else:
                path = RemoteFile(path=name, stats=stat)

            if match_all_filters(filters, path):
                result.append(path)

            if limits_reached(limits, path):
                break

        return result

    def walk(
        self,
        top: os.PathLike | str,
        topdown: bool = True,
        filters: Iterable[BaseFileFilter] | None = None,
        limits: Iterable[BaseFileLimit] | None = None,
    ) -> Iterator[tuple[RemoteDirectory, list[RemoteDirectory], list[RemoteFile]]]:

        filters = filters or []
        limits = limits or []

        for limit in limits:
            limit.reset()

        yield from self._walk(top, topdown=topdown, filters=filters, limits=limits)

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def rmdir(self, path: os.PathLike | str, recursive: bool = False) -> None:
        description = "RECURSIVELY" if recursive else "NON-recursively"
        log.debug(f"|{self.__class__.__name__}| {description} removing directory '{path}'")
        remote_directory = RemotePath(path)

        if not self.path_exists(remote_directory):
            log.debug(f"|{self.__class__.__name__}| Directory '{remote_directory}' does not exist, nothing to remove")
            return

        directory_info = path_repr(self.get_directory(remote_directory))
        if self.listdir(remote_directory) and not recursive:
            raise DirectoryNotEmptyError(
                f"|{self.__class__.__name__}| Cannot delete non-empty directory {directory_info}",
            )

        log.debug(f"|{self.__class__.__name__}| Directory to remove: {directory_info}")
        if recursive:
            self._rmdir_recursive(remote_directory)
        else:
            self._rmdir(remote_directory)

        log.info(f"|{self.__class__.__name__}| Successfully removed directory '{remote_directory}'")

    def _walk(  # noqa: WPS231
        self,
        top: os.PathLike | str,
        topdown: bool,
        filters: Iterable[BaseFileFilter],
        limits: Iterable[BaseFileLimit],
    ) -> Iterator[tuple[RemoteDirectory, list[RemoteDirectory], list[RemoteFile]]]:
        # no need to check nested directories if limit is already reached
        for limit in limits:
            if limit.is_reached:
                return

        log.debug(f"|{self.__class__.__name__}| Walking through directory '{top}'")
        root = self.get_directory(top)
        dirs, files = [], []

        for item in self._listdir(root):
            name = self._get_item_name(item)
            stat = self._get_item_stat(root, item)

            if self._is_item_dir(root, item):
                if not topdown:
                    yield from self._walk(top=root / name, topdown=topdown, filters=filters, limits=limits)

                path = RemoteDirectory(path=root / name, stats=stat)
                if match_all_filters(filters, path):
                    dirs.append(RemoteDirectory(path=name, stats=stat))

                if limits_reached(limits, path):
                    break
            else:
                path = RemoteFile(path=root / name, stats=stat)

                if match_all_filters(filters, path):
                    files.append(RemoteFile(path=name, stats=stat))

                if limits_reached(limits, path):
                    break

        if topdown:
            for name in dirs:
                yield from self._walk(top=root / name, topdown=topdown, filters=filters, limits=limits)

        log.debug(
            f"|{self.__class__.__name__}| "
            f"Directory '{root}' contains {len(dirs)} nested directories and {len(files)} files",
        )
        yield root, dirs, files

    def _rmdir_recursive(self, root: RemotePath) -> None:
        for item in self._listdir(root):
            name = self._get_item_name(item)
            stat = self._get_item_stat(root, item)

            if self._is_item_dir(root, item):
                path = RemoteDirectory(path=root / name, stats=stat)
                log.debug(f"|{self.__class__.__name__}| Directory to remove: {path_repr(path)}")
                self._rmdir_recursive(path)
                log.debug(f"|{self.__class__.__name__}| Successfully removed directory '{path}'")
            else:
                path = RemoteFile(path=root / name, stats=stat)
                log.debug(f"|{self.__class__.__name__}| File to remove: {path_repr(path)}")
                self._remove_file(path)
                log.debug(f"|{self.__class__.__name__}| Successfully removed file '{path}'")

        self._rmdir(root)

    def _get_item_name(self, item) -> str:
        return item

    def _is_item_dir(self, top: RemotePath, item) -> bool:
        return self._is_dir(top / self._get_item_name(item))

    def _is_item_file(self, top: RemotePath, item) -> bool:
        return self._is_file(top / self._get_item_name(item))

    def _get_item_stat(self, top: RemotePath, item) -> PathStatProtocol:
        return self._get_stat(top / self._get_item_name(item))

    def _log_parameters(self):
        log.info("|onETL| Using connection parameters:")
        log_with_indent(f"type = {self.__class__.__name__}")
        parameters = self.dict(by_alias=True, exclude_none=True)
        for attr, value in sorted(parameters.items()):
            if isinstance(value, os.PathLike):
                log_with_indent(f"{attr} = {path_repr(value)}")
            else:
                log_with_indent(f"{attr} = {value!r}")

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
    def _get_stat(self, path: RemotePath) -> PathStatProtocol:
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

    @abstractmethod
    def _is_dir(self, path: RemotePath) -> bool:
        """"""

    @abstractmethod
    def _is_file(self, path: RemotePath) -> bool:
        """"""
