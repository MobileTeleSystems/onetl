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
from typing import Iterable

from onetl.base.base_connection import BaseConnection
from onetl.base.base_file_filter import BaseFileFilter
from onetl.base.base_file_limit import BaseFileLimit
from onetl.base.path_protocol import PathProtocol, PathWithStatsProtocol
from onetl.base.path_stat_protocol import PathStatProtocol


class BaseFileConnection(BaseConnection):
    """
    Implements generic methods for files and directories manipulation on some filesystem (usually remote)
    """

    @abstractmethod
    def download_file(
        self,
        remote_file_path: os.PathLike | str,
        local_file_path: os.PathLike | str,
    ) -> PathWithStatsProtocol:
        """
        Downloads file from the remote filesystem to a local path
        """

    @abstractmethod
    def remove_file(self, remote_file_path: os.PathLike | str) -> None:
        """
        Removes file on remote filesystem
        """

    @abstractmethod
    def mkdir(self, path: os.PathLike | str) -> PathProtocol:
        """
        Creates directory tree on remote filesystem
        """

    @abstractmethod
    def upload_file(
        self,
        local_file_path: os.PathLike | str,
        remote_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> PathWithStatsProtocol:
        """
        Uploads local file to path on remote filesystem
        """

    @abstractmethod
    def rename_file(
        self,
        source_file_path: os.PathLike | str,
        target_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> PathWithStatsProtocol:
        """
        Rename or move file on remote filesystem
        """

    @abstractmethod
    def path_exists(self, path: os.PathLike | str) -> bool:
        """
        Check if specified path exists on remote filesystem
        """

    @abstractmethod
    def listdir(
        self,
        directory: os.PathLike | str,
        filters: Iterable[BaseFileFilter] | None = None,
        limits: Iterable[BaseFileLimit] | None = None,
    ) -> list[PathWithStatsProtocol]:
        """
        Return list of files in specific directory on remote filesystem
        """

    @abstractmethod
    def walk(
        self,
        top: os.PathLike | str,
        filters: Iterable[BaseFileFilter] | None = None,
        limits: Iterable[BaseFileLimit] | None = None,
    ) -> Iterable[tuple[PathWithStatsProtocol, list[PathWithStatsProtocol], list[PathWithStatsProtocol]]]:
        """
        Walk into directory tree on remote filesystem, just like ``os.walk``
        """

    @abstractmethod
    def rmdir(self, path: os.PathLike | str, recursive: bool = False) -> None:
        """
        Remove directory or directory tree on remote filesystem
        """

    @abstractmethod
    def is_file(self, path: os.PathLike | str) -> bool:
        """
        Check if specified path is a file on remote filesystem
        """

    @abstractmethod
    def is_dir(self, path: os.PathLike | str) -> bool:
        """
        Check if specified path is a directory on remote filesystem
        """

    @abstractmethod
    def get_stat(self, path: os.PathLike | str) -> PathStatProtocol:
        """
        Returns stats for a specific path on remote filesystem
        """

    @abstractmethod
    def get_directory(self, path: os.PathLike | str) -> PathWithStatsProtocol:
        """
        Returns file with stats for a specific path on remote filesystem
        """

    @abstractmethod
    def get_file(self, path: os.PathLike | str) -> PathWithStatsProtocol:
        """
        Returns file with stats for a specific path on remote filesystem
        """

    @abstractmethod
    def read_text(self, path: os.PathLike | str, encoding: str = "utf-8", **kwargs) -> str:
        """
        Returns string content of a file at specific path on remote filesystem
        """

    @abstractmethod
    def read_bytes(self, path: os.PathLike | str, **kwargs) -> bytes:
        """
        Returns binary content of a file at specific path on remote filesystem
        """

    @abstractmethod
    def write_text(
        self,
        path: os.PathLike | str,
        content: str,
        encoding: str = "utf-8",
        **kwargs,
    ) -> PathWithStatsProtocol:
        """
        Writes string to a specific path on remote filesystem
        """

    @abstractmethod
    def write_bytes(self, path: os.PathLike | str, content: bytes, **kwargs) -> PathWithStatsProtocol:
        """
        Writes bytes to a specific path on remote filesystem
        """

    @property
    @abstractmethod
    def instance_url(self):
        """Instance URL"""
