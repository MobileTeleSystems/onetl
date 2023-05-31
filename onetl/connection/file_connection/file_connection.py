#  Copyright 2023 MTS (Mobile Telesystems)
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
from onetl.exception import (
    DirectoryNotEmptyError,
    DirectoryNotFoundError,
    FileSizeMismatchError,
    NotAFileError,
)
from onetl.file.filter import match_all_filters
from onetl.file.limit import limits_reached, limits_stop_at, reset_limits
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


class FileConnection(BaseFileConnection, FrozenModel):
    _client: Any = None

    @property
    def client(self):
        if self._client and not self._is_client_closed():
            return self._client

        client = self._get_client()
        self._client = client
        return client

    def close(self):
        """
        Close all connections, opened by other methods call.

        Examples
        --------

        Get directory content and close connection:

        .. code:: python

            content = connection.list_dir("/mydir")
            assert content
            connection.close()

            # or

            with connection:
                content = connection.list_dir("/mydir")
                content = connection.list_dir("/mydir/abc")

        """

        if self._client:
            self._close_client()

        self._client = None

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()

        try:
            self.list_dir("/")
            log.info("|%s| Connection is available", self.__class__.__name__)
        except (RuntimeError, ValueError):
            # left validation errors intact
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise
        except Exception as e:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise RuntimeError("Connection is unavailable") from e

        return self

    def is_file(self, path: os.PathLike | str) -> bool:
        remote_path = RemotePath(path)

        if not self.path_exists(remote_path):
            raise FileNotFoundError(f"File '{remote_path}' does not exist")

        return self._is_file(remote_path)

    def is_dir(self, path: os.PathLike | str) -> bool:
        remote_path = RemotePath(path)

        if not self.path_exists(remote_path):
            raise DirectoryNotFoundError(f"Directory '{remote_path}' does not exist")

        return self._is_dir(remote_path)

    def get_stat(self, path: os.PathLike | str) -> PathStatProtocol:
        remote_path = RemotePath(path)
        return self._get_stat(remote_path)

    def resolve_dir(self, path: os.PathLike | str) -> RemoteDirectory:
        is_dir = self.is_dir(path)
        stat = self.get_stat(path)

        if not is_dir:
            raise NotADirectoryError(
                f"{path_repr(RemoteFile(path, stats=stat))} is not a directory",
            )

        return RemoteDirectory(path=path, stats=stat)

    def resolve_file(self, path: os.PathLike | str) -> RemoteFile:
        is_file = self.is_file(path)
        stat = self.get_stat(path)
        remote_path = RemoteFile(path=path, stats=stat)

        if not is_file:
            raise NotAFileError(f"{path_repr(remote_path)} is not a file")

        return remote_path

    def read_text(self, path: os.PathLike | str, encoding: str = "utf-8", **kwargs) -> str:
        log.debug(
            "|%s| Reading string with encoding %r and options %r from '%s'",
            self.__class__.__name__,
            encoding,
            kwargs,
            path,
        )

        remote_path = self.resolve_file(path)
        return self._read_text(remote_path, encoding=encoding, **kwargs)

    def read_bytes(self, path: os.PathLike | str, **kwargs) -> bytes:
        log.debug("|%s| Reading bytes with options %r from '%s'", self.__class__.__name__, kwargs, path)

        remote_path = self.resolve_file(path)
        return self._read_bytes(remote_path, **kwargs)

    def write_text(self, path: os.PathLike | str, content: str, encoding: str = "utf-8", **kwargs) -> RemoteFile:
        if not isinstance(content, str):
            raise TypeError(f"content must be str, not '{content.__class__.__name__}'")

        log.debug(
            "|%s| Writing string size %d with encoding %r and options %r to '%s'",
            self.__class__.__name__,
            len(content),
            encoding,
            kwargs,
            path,
        )

        remote_path = RemotePath(path)
        self.create_dir(remote_path.parent)

        if self.path_exists(remote_path):
            file = self.resolve_file(remote_path)
            log.warning(
                "|%s| File %s already exists and will be overwritten",
                self.__class__.__name__,
                path_repr(file),
            )

        self._write_text(remote_path, content=content, encoding=encoding, **kwargs)

        return self.resolve_file(remote_path)

    def write_bytes(self, path: os.PathLike | str, content: bytes, **kwargs) -> RemoteFile:
        if not isinstance(content, bytes):
            raise TypeError(f"content must be bytes, not '{content.__class__.__name__}'")

        log.debug(
            "|%s| Writing %s with options %e to '%s'",
            self.__class__.__name__,
            naturalsize(len(content)),
            kwargs,
            path,
        )

        remote_path = RemotePath(path)
        self.create_dir(remote_path.parent)

        if self.path_exists(remote_path):
            file = self.resolve_file(remote_path)
            log.warning(
                "|%s| File %s already exists and will be overwritten",
                self.__class__.__name__,
                path_repr(file),
            )

        self._write_bytes(remote_path, content=content, **kwargs)

        return self.resolve_file(remote_path)

    def download_file(
        self,
        remote_file_path: os.PathLike | str,
        local_file_path: os.PathLike | str,
        replace: bool = True,
    ) -> LocalPath:
        log.debug(
            "|%s| Downloading file '%s' to local '%s'",
            self.__class__.__name__,
            remote_file_path,
            local_file_path,
        )

        remote_file = self.resolve_file(remote_file_path)
        local_file = LocalPath(local_file_path)

        if local_file.exists():
            if not local_file.is_file():
                raise NotAFileError(f"{path_repr(local_file)} is not a file")

            if not replace:
                raise FileExistsError(f"File {path_repr(local_file)} already exists")

            log.warning("|Local FS| File %s already exists, overwriting", path_repr(local_file))
            local_file.unlink()

        log.debug("|Local FS| Creating target directory '%s'", local_file.parent)
        local_file.parent.mkdir(parents=True, exist_ok=True)
        self._download_file(remote_file, local_file)

        if local_file.stat().st_size != remote_file.stat().st_size:
            raise FileSizeMismatchError(
                f"The size of the downloaded file ({naturalsize(local_file.stat().st_size)}) does not match "
                f"the size of the file on the source ({naturalsize(remote_file.stat().st_size)})",
            )

        log.info("|Local FS| Successfully downloaded file '%s'", local_file)
        return local_file

    def remove_file(self, path: os.PathLike | str) -> bool:
        log.debug("|%s| Removing file '%s'", self.__class__.__name__, path)

        if not self.path_exists(path):
            log.debug("|%s| File '%s' does not exist, nothing to remove", self.__class__.__name__, path)
            return False

        file = self.resolve_file(path)
        log.debug("|%s| File to remove: %s", self.__class__.__name__, path_repr(file))

        self._remove_file(file)
        log.info("|%s| Successfully removed file '%s'", self.__class__.__name__, file)
        return True

    def create_dir(self, path: os.PathLike | str) -> RemoteDirectory:
        log.debug("|%s| Creating directory '%s'", self.__class__.__name__, path)
        remote_dir = RemotePath(path)

        if self.path_exists(remote_dir):
            return self.resolve_dir(remote_dir)

        self._create_dir(remote_dir)
        log.info("|%s| Successfully created directory '%s'", self.__class__.__name__, remote_dir)
        return self.resolve_dir(remote_dir)

    def upload_file(
        self,
        local_file_path: os.PathLike | str,
        remote_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> RemoteFile:
        log.debug("|%s| Uploading local file '%s' to '%s'", self.__class__.__name__, local_file_path, remote_file_path)

        local_file = LocalPath(local_file_path)
        if not local_file.exists():
            raise FileNotFoundError(f"File '{local_file}' does not exist")

        if not local_file.is_file():
            raise NotAFileError(f"{path_repr(local_file)} is not a file")

        remote_file = RemotePath(remote_file_path)
        if self.path_exists(remote_file):
            file = self.resolve_file(remote_file_path)
            if not replace:
                raise FileExistsError(f"File {path_repr(file)} already exists")

            log.warning("|%s| File %s already exists, overwriting", self.__class__.__name__, path_repr(file))
            self._remove_file(remote_file)

        self.create_dir(remote_file.parent)

        self._upload_file(local_file, remote_file)
        result = self.resolve_file(remote_file)

        if result.stat().st_size != local_file.stat().st_size:
            raise FileSizeMismatchError(
                f"The size of the uploaded file ({naturalsize(result.stat().st_size)}) does not match "
                f"the size of the file on the source ({naturalsize(local_file.stat().st_size)})",
            )

        log.info("|%s| Successfully uploaded file '%s'", self.__class__.__name__, remote_file)
        return result

    def rename_file(
        self,
        source_file_path: os.PathLike | str,
        target_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> RemoteFile:
        log.debug("|%s| Renaming file '%s' to '%s'", self.__class__.__name__, source_file_path, target_file_path)

        source_file = self.resolve_file(source_file_path)
        target_file = RemotePath(target_file_path)

        if self.path_exists(target_file):
            file = self.resolve_file(target_file)
            if not replace:
                raise FileExistsError(f"File {path_repr(file)} already exists")

            log.warning("|%s| File %s already exists, overwriting", self.__class__.__name__, path_repr(file))
            self._remove_file(target_file)

        self.create_dir(target_file.parent)
        self._rename_file(source_file, target_file)
        log.info("|%s| Successfully renamed file '%s' to '%s'", self.__class__.__name__, source_file, target_file)

        return self.resolve_file(target_file)

    def list_dir(
        self,
        path: os.PathLike | str,
        filters: Iterable[BaseFileFilter] | None = None,
        limits: Iterable[BaseFileLimit] | None = None,
    ) -> list[RemoteDirectory | RemoteFile]:
        log.debug("|%s| Listing directory '%s'", self.__class__.__name__, path)
        remote_dir = self.resolve_dir(path)
        result: list[RemoteDirectory | RemoteFile] = []

        filters = filters or []
        limits = reset_limits(limits or [])

        for entry in self._scan_entries(remote_dir):
            name = self._extract_name_from_entry(entry)
            stat = self._extract_stat_from_entry(remote_dir, entry)

            if self._is_dir_entry(remote_dir, entry):
                path = RemoteDirectory(path=name, stats=stat)
            else:
                path = RemoteFile(path=name, stats=stat)

            if match_all_filters(path, filters):
                result.append(path)

            if limits_stop_at(path, limits):
                break

        return result

    def walk(
        self,
        root: os.PathLike | str,
        topdown: bool = True,
        filters: Iterable[BaseFileFilter] | None = None,
        limits: Iterable[BaseFileLimit] | None = None,
    ) -> Iterator[tuple[RemoteDirectory, list[RemoteDirectory], list[RemoteFile]]]:
        root_dir = self.resolve_dir(root)

        filters = filters or []
        limits = reset_limits(limits or [])
        yield from self._walk(root_dir, topdown=topdown, filters=filters, limits=limits)

    def remove_dir(self, path: os.PathLike | str, recursive: bool = False) -> bool:
        description = "RECURSIVELY" if recursive else "NON-recursively"
        log.debug("|%s| %s removing directory '%s'", self.__class__.__name__, description, path)
        remote_dir = RemotePath(path)

        if not self.path_exists(remote_dir):
            log.debug(
                "|%s| Directory '%s' does not exist, nothing to remove",
                self.__class__.__name__,
                remote_dir,
            )
            return False

        directory_info = path_repr(self.resolve_dir(remote_dir))
        if self.list_dir(remote_dir) and not recursive:
            raise DirectoryNotEmptyError(
                "|%s| Cannot delete non-empty directory %s",
                self.__class__.__name__,
                directory_info,
            )

        log.debug("|%s| Directory to remove: %s", self.__class__.__name__, directory_info)
        if recursive:
            self._remove_dir_recursive(remote_dir)
        else:
            self._remove_dir(remote_dir)

        log.info("|%s| Successfully removed directory '%s'", self.__class__.__name__, remote_dir)
        return True

    def _walk(  # noqa: WPS231
        self,
        root: RemoteDirectory,
        topdown: bool,
        filters: Iterable[BaseFileFilter],
        limits: Iterable[BaseFileLimit],
    ) -> Iterator[tuple[RemoteDirectory, list[RemoteDirectory], list[RemoteFile]]]:
        # no need to check nested directories if limit is already reached
        if limits_reached(limits):
            return

        log.debug("|%s| Walking through directory '%s'", self.__class__.__name__, root)
        dirs, files = [], []

        for entry in self._scan_entries(root):
            name = self._extract_name_from_entry(entry)
            stat = self._extract_stat_from_entry(root, entry)

            if self._is_dir_entry(root, entry):
                if not topdown:
                    yield from self._walk(root=root / name, topdown=topdown, filters=filters, limits=limits)

                path = RemoteDirectory(path=root / name, stats=stat)
                if match_all_filters(path, filters):
                    dirs.append(RemoteDirectory(path=name, stats=stat))

                    if limits_stop_at(path, limits):
                        break
            else:
                path = RemoteFile(path=root / name, stats=stat)

                if match_all_filters(path, filters):
                    files.append(RemoteFile(path=name, stats=stat))

                    if limits_stop_at(path, limits):
                        break

        if topdown:
            for name in dirs:
                yield from self._walk(root=root / name, topdown=topdown, filters=filters, limits=limits)

        log.debug(
            "|%s| Directory '%s' contains %d nested directories and %d files",
            self.__class__.__name__,
            root,
            len(dirs),
            len(files),
        )
        yield root, dirs, files

    def _remove_dir_recursive(self, root: RemotePath) -> None:
        for entry in self._scan_entries(root):
            name = self._extract_name_from_entry(entry)
            stat = self._extract_stat_from_entry(root, entry)

            if self._is_dir_entry(root, entry):
                path = RemoteDirectory(path=root / name, stats=stat)
                log.debug("|%s| Directory to remove: %s", self.__class__.__name__, path_repr(path))
                self._remove_dir_recursive(path)
                log.debug("|%s| Successfully removed directory '%s'", self.__class__.__name__, path)
            else:
                path = RemoteFile(path=root / name, stats=stat)
                log.debug("|%s| File to remove: %s", self.__class__.__name__, path_repr(path))
                self._remove_file(path)
                log.debug("|%s| Successfully removed file '%s'", self.__class__.__name__, path)

        self._remove_dir(root)

    @abstractmethod
    def _scan_entries(self, path: RemotePath) -> list:
        """
        The method returns a list that contains entries.

        Entry is an object containing information about a path, like file or nested directory.

        The entry contains information like file/directory name, file size, modification time, owner,
        POSIX-compatible permissions and so on. The only required attribute is file name, others can be omitted.

        Parameters
        ----------
        path : RemotePath
            Path to the source directory

        Returns
        -------
        list
            List of the entries

        Examples
        --------

        Get a list of entries:

        .. code:: python

            entry_list = connection._scan_entries(path="/a/path/to/the/directory")

            print(entry_list)

            [
                {
                    "created": "2023-12-08T18:33:39Z",
                    "owner": None,
                    "size": "23",
                    "modified": "2023-12-08 18:33:20",
                    "isdir": False,
                    "path": "/path/to/the/file.txt",
                },
            ]

        """

    @abstractmethod
    def _extract_name_from_entry(self, entry) -> str:
        """
        Return file or directory name from the entry object.

        Parameters
        ----------
        entry
            One of the elements retrieved from the list (returned by :obj:`~_scan_entries`).

        Returns
        -------
        str

        Examples
        --------

        Get an entry name:

        .. code:: python

            for entry in connection._scan_entries(path="/a/path/to/the/directory"):
                assert entry == {
                    "created": "2023-12-08T18:33:39Z",
                    "owner": None,
                    "size": "23",
                    "modified": "2023-12-08 18:33:20",
                    "isdir": False,
                    "path": "/path/to/the/file.txt",
                }

                assert entry._extract_name_from_entry(entry) == "file.txt"
        """

    @abstractmethod
    def _is_dir_entry(self, top: RemotePath, entry) -> bool:
        """
        Returns ``True`` if the object that describes the entry is a directory.

        If entry object does not contain such information, you could construct a path
        from ``top / entry.name`` and pass it into :obj:`~_is_dir` method.
        But this should be avoided because such implementation sends multiple requests per file.

        Parameters
        ----------
        top : RemotePath
            Root directory
        entry
            One of the elements retrieved from the list (returned by :obj:`~_scan_entries`).

        Returns
        -------
        bool

        Examples
        --------

        Show if the entry is a directory:

        .. code:: python

            for component in connection._scan_entries(path="/a/path/to/the/directory"):
                assert connection._is_dir_entry(root="/a/path/to/the/directory", entry) == True
        """

    @abstractmethod
    def _is_file_entry(self, top: RemotePath, entry) -> bool:
        """
        Returns ``True`` if the object that describes the entry is a file.

        If entry object does not contain such information, you could construct a path
        from ``top / entry.name`` and pass it into :obj:`~_is_file` method.
        But this should be avoided because such implementation sends multiple requests per file.

        Parameters
        ----------
        top : RemotePath
            Root directory
        entry
            One of the elements retrieved from the list (returned by :obj:`~_scan_entries`).

        Returns
        -------
        bool

        Examples
        --------

        Show if the entry is a file:

        .. code:: python

            for entry in connection._scan_entries(path="/a/path/to/the/directory"):
                assert connection._is_file_entry(root="/a/path/to/the/directory", entry) == True
        """

    @abstractmethod
    def _extract_stat_from_entry(self, top: RemotePath, entry) -> PathStatProtocol:
        """
        Returns an object containing information about file size, modification date,
        owner, POSIX-compatible permissions and so on.

        Object should be compatible with :obj:`onetl.base.path_stat_protocol.PathStatProtocol` interface.

        If entry object does not contain such information, you could construct a path
        from ``top / entry.name`` and pass it into :obj:`~_get_stat` method.
        But this should be avoided because such implementation sends multiple requests per file.

        Parameters
        ----------
        top : RemotePath
            Root directory.
        entry
            One of the elements retrieved from the list (returned by :obj:`~_scan_entries`).

        Returns
        -------
        PathStatProtocol

        Examples
        --------

        Get statistics object from the entry:

        .. code:: python

            for entry in connection._scan_entries(path="/a/path/to/the/directory"):
                stat = connection._extract_stat_from_entry(root="/a/path/to/the/directory", entry)

                assert stat == RemotePathStat(
                    st_size=23, st_mtime=1670517693.0, st_mode=None, st_uid=None, st_gid=None
                )

        """

    def _log_parameters(self):
        log.info("|onETL| Using connection parameters:")
        log_with_indent("type = %s", self.__class__.__name__)
        parameters = self.dict(by_alias=True, exclude_none=True)
        for attr, value in sorted(parameters.items()):
            if isinstance(value, os.PathLike):
                log_with_indent("%s = %s", attr, path_repr(value))
            else:
                log_with_indent("%s = %r", attr, value)

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
    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        """"""

    @abstractmethod
    def _get_stat(self, path: RemotePath) -> PathStatProtocol:
        """"""

    @abstractmethod
    def _remove_file(self, remote_file_path: RemotePath) -> None:
        """"""

    @abstractmethod
    def _create_dir(self, path: RemotePath) -> None:
        """"""

    @abstractmethod
    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        """"""

    @abstractmethod
    def _rename_file(self, source: RemotePath, target: RemotePath) -> None:
        """"""

    @abstractmethod
    def _remove_dir(self, path: RemotePath) -> None:
        """"""

    @abstractmethod
    def _read_text(self, path: RemotePath, encoding: str) -> str:
        """"""

    @abstractmethod
    def _read_bytes(self, path: RemotePath) -> bytes:
        """"""

    @abstractmethod
    def _write_text(self, path: RemotePath, content: str, encoding: str) -> None:
        """"""

    @abstractmethod
    def _write_bytes(self, path: RemotePath, content: bytes) -> None:
        """"""

    @abstractmethod
    def _is_dir(self, path: RemotePath) -> bool:
        """"""

    @abstractmethod
    def _is_file(self, path: RemotePath) -> bool:
        """"""
