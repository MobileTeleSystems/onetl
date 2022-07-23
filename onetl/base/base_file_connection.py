from __future__ import annotations

import os
from abc import abstractmethod
from dataclasses import dataclass
from typing import Iterable

from onetl.base.base_connection import BaseConnection
from onetl.base.base_file_filter import BaseFileFilter
from onetl.base.file_stat_protocol import FileStatProtocol
from onetl.base.path_protocol import PathProtocol, SizedPathProtocol


@dataclass(frozen=True)
class BaseFileConnection(BaseConnection):
    """
    Implements generic methods for files and directories manipulation on some filesystem (usually remote)
    """

    @abstractmethod
    def download_file(
        self,
        remote_file_path: os.PathLike | str,
        local_file_path: os.PathLike | str,
    ) -> SizedPathProtocol:
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
    ) -> SizedPathProtocol:
        """
        Uploads local file to path on remote filesystem
        """

    @abstractmethod
    def rename_file(
        self,
        source_file_path: os.PathLike | str,
        target_file_path: os.PathLike | str,
        replace: bool = False,
    ) -> SizedPathProtocol:
        """
        Rename or move file on remote filesystem
        """

    @abstractmethod
    def path_exists(self, path: os.PathLike | str) -> bool:
        """
        Check if specified path exists on remote filesystem
        """

    @abstractmethod
    def listdir(self, path: os.PathLike | str) -> list[PathProtocol]:
        """
        Return list of files in specific directory on remote filesystem
        """

    @abstractmethod
    def walk(
        self,
        top: os.PathLike | str,
        filter: BaseFileFilter | None = None,  # noqa: WPS125
    ) -> Iterable[tuple[PathProtocol, list[PathProtocol], list[SizedPathProtocol]]]:
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
    def get_stat(self, path: os.PathLike | str) -> FileStatProtocol:
        """
        Returns stats for a specific path on remote filesystem
        """

    @abstractmethod
    def get_file(self, path: os.PathLike | str) -> SizedPathProtocol:
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
    def write_text(self, path: os.PathLike | str, content: str, encoding: str = "utf-8", **kwargs) -> SizedPathProtocol:
        """
        Writes string to a specific path on remote filesystem
        """

    @abstractmethod
    def write_bytes(self, path: os.PathLike | str, content: bytes, **kwargs) -> SizedPathProtocol:
        """
        Writes bytes to a specific path on remote filesystem
        """

    @property
    @abstractmethod
    def instance_url(self):
        """Instance URL"""
