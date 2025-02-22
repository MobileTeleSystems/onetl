# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from abc import abstractmethod
from typing import Iterable

from onetl.base.base_connection import BaseConnection
from onetl.base.base_file_filter import BaseFileFilter
from onetl.base.base_file_limit import BaseFileLimit
from onetl.base.path_protocol import PathWithStatsProtocol
from onetl.base.path_stat_protocol import PathStatProtocol


class BaseFileConnection(BaseConnection):
    """
    Implements generic methods for files and directories manipulation on some filesystem (usually remote).

    .. versionadded:: 0.8.0
    """

    @abstractmethod
    def path_exists(self, path: os.PathLike | str) -> bool:
        """
        Check if specified path exists on remote filesystem. |support_hooks|.

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            Path to check

        Returns
        -------
        ``True`` if path exists, ``False`` otherwise

        Examples
        --------

        >>> connection.path_exists("/path/to/file.csv")
        True
        >>> connection.path_exists("/path/to/dir")
        True
        >>> connection.path_exists("/path/to/missing")
        False
        """

    @abstractmethod
    def is_file(self, path: os.PathLike | str) -> bool:
        """
        Check if specified path is a file. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            Path to check

        Returns
        -------
        ``True`` if path is a file, ``False`` otherwise.

        Raises
        ------
        FileNotFoundError
            Path does not exist

        Examples
        --------

        >>> connection.is_file("/path/to/dir/file.csv")
        True
        >>> connection.is_file("/path/to/dir")
        False
        """

    @abstractmethod
    def is_dir(self, path: os.PathLike | str) -> bool:
        """
        Check if specified path is a directory. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            Path to check

        Returns
        -------
        ``True`` if path is a directory, ``False`` otherwise.

        Raises
        ------
        :obj:`onetl.exception.DirectoryNotFoundError`
            Path does not exist

        Examples
        --------

        >>> connection.is_dir("/path/to/dir")
        True
        >>> connection.is_dir("/path/to/dir/file.csv")
        False
        """

    @abstractmethod
    def get_stat(self, path: os.PathLike | str) -> PathStatProtocol:
        """
        Returns stats for a specific path. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            Path to get stats for

        Returns
        -------
        Stats object

        Raises
        ------
        Any underlying client exception

        Examples
        --------

        >>> stat = connection.get_stat("/path/to/file.csv")
        >>> stat.st_size  # in bytes
        1024
        >>> stat.st_uid  # owner id or name
        12345
        """

    @abstractmethod
    def resolve_dir(self, path: os.PathLike | str) -> PathWithStatsProtocol:
        """
        Returns directory at specific path, with stats. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            Path to resolve

        Returns
        -------
        Directory path with stats

        Raises
        ------
        :obj:`onetl.exception.DirectoryNotFoundError`
            Path does not exist

        NotADirectoryError
            Path is not a directory

        Examples
        --------

        >>> dir_path = connection.resolve_dir("/path/to/dir")
        >>> os.fspath(dir_path)
        '/path/to/dir'
        >>> dir_path.stat().st_uid  # owner id
        12345
        """

    @abstractmethod
    def resolve_file(self, path: os.PathLike | str) -> PathWithStatsProtocol:
        """
        Returns file at specific path, with stats. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            Path to resolve

        Returns
        -------
        File path with stats

        Raises
        ------
        FileNotFoundError
            Path does not exist

        :obj:`onetl.exception.NotAFileError`
            Path is not a file

        Examples
        --------

        >>> file_path = connection.resolve_file("/path/to/dir/file.csv")
        >>> os.fspath(file_path)
        '/path/to/dir/file.csv'
        >>> file_path.stat().st_uid  # owner id
        12345
        """

    @abstractmethod
    def create_dir(self, path: os.PathLike | str) -> PathWithStatsProtocol:
        """
        Creates directory tree on remote filesystem. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            Directory path

        Returns
        -------
        Created directory with stats

        Raises
        ------
        :obj:`onetl.exception.NotAFileError`
            Path is not a file

        Examples
        --------

        >>> dir_path = connection.create_dir("/path/to/dir")
        >>> os.fspath(dir_path)
        '/path/to/dir'
        """

    @abstractmethod
    def remove_file(self, path: os.PathLike | str) -> bool:
        """
        Removes file on remote filesystem. |support_hooks|

        If file does not exist, no exception is raised.

        .. warning::

            Supports only one file removal per call. Directory removal is **NOT** supported, use :obj:`~remove_dir` instead.

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            File path

        Returns
        -------
        ``True`` if file was removed, ``False`` if file does not exist in the first place.

        Raises
        ------
        :obj:`onetl.exception.NotAFileError`
            Path is not a file

        Examples
        --------

        >>> connection.remove_file("/path/to/file.csv")
        True
        >>> connection.path_exists("/path/to/dir/file.csv")
        False
        >>> connection.remove_file("/path/to/file.csv")  # already deleted, no error
        False
        """

    @abstractmethod
    def remove_dir(self, path: os.PathLike | str, recursive: bool = False) -> bool:
        """
        Remove directory or directory tree. |support_hooks|

        If directory does not exist, no exception is raised.

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            Directory path to remote

        recursive : bool, default ``False``
            If ``True``, remove directory tree recursively.

        Returns
        -------
        ``True`` if directory was removed, ``False`` if directory does not exist in the first place.

        Raises
        ------
        NotADirectoryError
            Path is not a directory

        Examples
        --------

        >>> connection.remove_dir("/path/to/dir")
        True
        >>> connection.path_exists("/path/to/dir")
        False
        >>> connection.path_exists("/path/to/dir/file.csv")
        False
        >>> connection.remove_dir("/path/to/dir")  # already deleted, no error
        False
        """

    @abstractmethod
    def rename_file(
        self,
        source_file_path: os.PathLike | str,
        target_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> PathWithStatsProtocol:
        """
        Rename or move file on remote filesystem. |support_hooks|

        .. warning::

            Supports only one file move per call. Directory move/rename is **NOT** supported.

        .. versionadded:: 0.8.0

        Parameters
        ----------
        source_file_path : str or :obj:`os.PathLike`
            Old file path

        target_file_path : str or :obj:`os.PathLike`
            New file path

        replace : bool, default ``False``
            If ``True``, existing file will be replaced.

        Returns
        -------
        New file path with stats.

        Raises
        ------
        :obj:`onetl.exception.NotAFileError`
            Source or target path is not a file

        FileNotFoundError
            File does not exist

        FileExistsError
            File already exists, and ``replace=False``

        Examples
        --------

        >>> new_file = connection.rename_file("/path/to/file1.csv", "/path/to/file2.csv")
        >>> os.fspath(new_file)
        '/path/to/file2.csv'
        >>> connection.path_exists("/path/to/file2.csv")
        True
        >>> connection.path_exists("/path/to/file1.csv")
        False
        """

    @abstractmethod
    def list_dir(
        self,
        path: os.PathLike | str,
        filters: Iterable[BaseFileFilter] | None = None,
        limits: Iterable[BaseFileLimit] | None = None,
    ) -> list[PathWithStatsProtocol]:
        """
        Return list of child files/directories in a specific directory. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            Directory path to list contents.

        filters : list of :obj:`BaseFileFilter <onetl.base.base_file_filter.BaseFileFilter>`, optional
            Return only files/directories matching these filters. See :ref:`file-filters`

        limits : list of :obj:`BaseFileLimit <onetl.base.base_file_limit.BaseFileLimit>`, optional
            Apply limits to the list of files/directories, and stop if one of the limits is reached.
            See :ref:`file-limits`

        Returns
        -------
        List of :obj:`onetl.base.PathWithStatsProtocol`

        Raises
        ------
        NotADirectoryError
            Path is not a directory

        :obj:`onetl.exception.DirectoryNotFoundError`
            Path does not exist

        Examples
        --------

        >>> dir_content = connection.list_dir("/path/to/dir")
        >>> os.fspath(dir_content[0])
        'file.csv'
        >>> connection.path_exists("/path/to/dir/file.csv")
        True
        """

    @abstractmethod
    def walk(
        self,
        root: os.PathLike | str,
        topdown: bool = True,
        filters: Iterable[BaseFileFilter] | None = None,
        limits: Iterable[BaseFileLimit] | None = None,
    ) -> Iterable[tuple[PathWithStatsProtocol, list[PathWithStatsProtocol], list[PathWithStatsProtocol]]]:
        """
        Walk into directory tree, and iterate over its content in all nesting levels. |support_hooks|

        Just like :obj:`os.walk`, but with additional filter/limit logic.

        .. versionadded:: 0.8.0

        Parameters
        ----------
        root : str or :obj:`os.PathLike`
            Directory path to walk into.

        topdown : bool, default ``True``
            If ``True``, walk in top-down order, otherwise walk in bottom-up order.

        filters : list of :obj:`BaseFileFilter <onetl.base.base_file_filter.BaseFileFilter>`, optional
            Return only files/directories matching these filters. See :ref:`file-filters`.

        limits : list of :obj:`BaseFileLimit <onetl.base.base_file_limit.BaseFileLimit>`, optional
            Apply limits to the list of files/directories, and immediately stop if any of these limits is reached.
            See :ref:`file-limits`.

        Returns
        -------
        ``Iterator[tuple[root, dirs, files]]``, like :obj:`os.walk`.

        But all the paths are not strings, instead path classes with embedded stats are returned.

        Raises
        ------
        NotADirectoryError
            Path is not a directory

        :obj:`onetl.exception.DirectoryNotFoundError`
            Path does not exist

        Examples
        --------

        >>> for root, dirs, files in connection.walk("/path/to/dir"):
        ...    break
        >>> os.fspath(root)
        '/path/to/dir'
        >>> dirs
        []
        >>> os.fspath(files[0])
        'file.csv'
        >>> connection.path_exists("/path/to/dir/file.csv")
        True
        """

    @abstractmethod
    def download_file(
        self,
        remote_file_path: os.PathLike | str,
        local_file_path: os.PathLike | str,
        replace: bool = True,
    ) -> PathWithStatsProtocol:
        """
        Downloads file from the remote filesystem to a local path. |support_hooks|

        .. warning::

            Supports only one file download per call. Directory download is **NOT** supported, use :ref:`file-downloader` instead.

        .. versionadded:: 0.8.0

        Parameters
        ----------
        remote_file_path : str or :obj:`os.PathLike`
            Remote file path to read from

        local_file_path : str or :obj:`os.PathLike`
            Local file path to create

        replace : bool, default ``False``
            If ``True``, existing file will be replaced

        Returns
        -------
        Local file with stats.

        Raises
        ------
        :obj:`onetl.exception.NotAFileError`
            Remote or local path is not a file

        FileNotFoundError
            Remote file does not exist

        FileExistsError
            Local file already exists, and ``replace=False``

        :obj:`onetl.exception.FileSizeMismatchError`
            Target file size after download is different from source file size.

        Examples
        --------

        >>> local_file = connection.download_file(
        ...     remote_file_path="/path/to/source.csv",
        ...     local_file_path="/path/to/target.csv",
        ... )
        >>> os.fspath(local_file)
        '/path/to/target.csv'
        >>> local_file.exists()
        True
        >>> local_file.stat().st_size  # in bytes
        1024
        >>> connection.get_stat("/path/to/source.csv").st_size  # same size
        1024
        """

    @abstractmethod
    def upload_file(
        self,
        local_file_path: os.PathLike | str,
        remote_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> PathWithStatsProtocol:
        """
        Uploads local file to a remote filesystem. |support_hooks|

        .. warning::

            Supports only one file upload per call. Directory upload is **NOT** supported, use :ref:`file-uploader` instead.

        .. versionadded:: 0.8.0

        Parameters
        ----------
        local_file_path : str or :obj:`os.PathLike`
            Local file path to read from

        remote_file_path : str or :obj:`os.PathLike`
            Remote file path to create

        replace : bool, default ``False``
            If ``True``, existing file will be replaced

        Returns
        -------
        Remote file with stats.

        Raises
        ------
        :obj:`onetl.exception.NotAFileError`
            Remote or local path is not a file

        FileNotFoundError
            Local file does not exist

        FileExistsError
            Remote file already exists, and ``replace=False``

        :obj:`onetl.exception.FileSizeMismatchError`
            Target file size after upload is different from source file size.

        Examples
        --------

        >>> remote_file = connection.upload(
        ...     local_file_path="/path/to/source.csv",
        ...     remote_file_path="/path/to/target.csv",
        ... )
        >>> os.fspath(remote_file)
        '/path/to/target.csv'
        >>> connection.path_exists("/path/to/target.csv")
        True
        >>> remote_file.stat().st_size  # in bytes
        1024
        >>> os.stat("/path/to/source.csv").st_size  # same as source
        1024
        """

    @abstractmethod
    def read_text(self, path: os.PathLike | str, encoding: str = "utf-8") -> str:
        r"""
        Returns string content of a file at specific path. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            File path to read

        encoding : str, default ``utf-8``
            File content encoding

        Returns
        -------
        File content

        Raises
        ------
        FileNotFoundError
            Path does not exist

        :obj:`onetl.exception.NotAFileError`
            Path is not a file

        Examples
        --------

        >>> connection.read_text("/path/to/dir/file.csv")
        'some;header\n1;2'
        """

    @abstractmethod
    def read_bytes(self, path: os.PathLike | str) -> bytes:
        """
        Returns binary content of a file at specific path. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            File path to read

        Returns
        -------
        File content

        Raises
        ------
        FileNotFoundError
            Path does not exist

        :obj:`onetl.exception.NotAFileError`
            Path is not a file

        Examples
        --------

        >>> connection.read_bytes("/path/to/dir/file.csv")
        b'0xdeadbeef'
        """

    @abstractmethod
    def write_text(
        self,
        path: os.PathLike | str,
        content: str,
        encoding: str = "utf-8",
    ) -> PathWithStatsProtocol:
        r"""
        Writes string content to a file at specific path. |support_hooks|

        .. warning::

            If file already exists, its content will be replaced.

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            File path to write

        content : str
            File content

        encoding : str, default ``utf-8``
            File content encoding

        Returns
        -------
        File path with stats after write

        Raises
        ------
        TypeError
            Content is not a string

        :obj:`onetl.exception.NotAFileError`
            Path is not a file

        Examples
        --------

        >>> file_path = connection.write_text("/path/to/dir/file.csv", "some;header\n1;2")
        >>> os.fspath(file_path)
        '/path/to/dir/file.csv'
        >>> file_path.stat().st_size  # in bytes
        1024
        """

    @abstractmethod
    def write_bytes(self, path: os.PathLike | str, content: bytes) -> PathWithStatsProtocol:
        """
        Writes bytes content to a file at specific path. |support_hooks|

        .. warning::

            If file already exists, its content will be replaced.

        .. versionadded:: 0.8.0

        Parameters
        ----------
        path : str or :obj:`os.PathLike`
            File path to write

        content : bytes
            File content

        Returns
        -------
        File path with stats after write

        Raises
        ------
        TypeError
            Content is not a string

        :obj:`onetl.exception.NotAFileError`
            Path is not a file

        Examples
        --------

        >>> file_path = connection.write_bytes("/path/to/dir/file.csv", b"0xdeadbeef")
        >>> os.fspath(file_path)
        '/path/to/dir/file.csv'
        >>> file_path.stat().st_size  # in bytes
        1024
        """

    @property
    @abstractmethod
    def instance_url(self):
        """Instance URL"""
