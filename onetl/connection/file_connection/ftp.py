from __future__ import annotations

import ftplib  # noqa: S402
import os
from dataclasses import dataclass
from logging import getLogger

from ftputil import FTPHost
from ftputil import session as ftp_session

from onetl.base import FileStatProtocol
from onetl.connection.file_connection.file_connection import FileConnection
from onetl.impl import LocalPath, RemotePath

log = getLogger(__name__)


# TODO: (@mivasil6) подумать на что можно поменять слова source/target
@dataclass(frozen=True)
class FTP(FileConnection):
    """Class for FTP file connection.

    Parameters
    ----------
    host : str
        Host of ftp source. For example: ``0001testadviat04.msk.mts.ru``
    port : int, default: ``21``
        Port of ftp source
    user : str
        User, which have access to the file source. For example: ``sa0000sphrsftptest``
    password : str, default: ``None``
        Password for file source connection

    Examples
    --------

    FTP file connection initialization

    .. code::

        from onetl.connection import FTP

        ftp = FTP(
            host="0001testadviat04.msk.mts.ru",
            user="sa0000sphrsftptest",
            password="*****",
        )
    """

    port: int = 21

    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.path.exists(os.fspath(path))

    def _get_client(self) -> FTPHost:
        """
        Returns a FTP connection object
        """

        session_factory = ftp_session.session_factory(
            base_class=ftplib.FTP,
            port=self.port,
            encrypt_data_channel=True,
            debug_level=0,
        )

        return FTPHost(
            self.host,
            self.user,
            self.password,
            session_factory=session_factory,
        )

    def _is_client_closed(self) -> bool:
        return self._client.closed

    def _close_client(self) -> None:
        self._client.close()

    def _rmdir_recursive(self, root: RemotePath) -> None:
        self.client.rmtree(os.fspath(root))

    def _rmdir(self, path: RemotePath) -> None:
        self.client.rmdir(os.fspath(path))

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        self.client.upload(os.fspath(local_file_path), os.fspath(remote_file_path))

    def _rename(self, source: RemotePath, target: RemotePath) -> None:
        self.client.rename(os.fspath(source), os.fspath(target))

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        self.client.download(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.remove(os.fspath(remote_file_path))

    def _mkdir(self, path: RemotePath) -> None:
        self.client.makedirs(os.fspath(path), exist_ok=True)

    def _listdir(self, path: RemotePath) -> list:
        return self.client.listdir(os.fspath(path))

    def _is_dir(self, path: RemotePath) -> bool:
        return self.client.path.isdir(os.fspath(path))

    def _is_file(self, path: RemotePath) -> bool:
        return self.client.path.isfile(os.fspath(path))

    def _get_stat(self, path: RemotePath) -> FileStatProtocol:
        return self.client.stat(os.fspath(path))

    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        with self.client.open(os.fspath(path), mode="r", encoding=encoding, **kwargs) as file:
            return file.read()

    def _read_bytes(self, path: RemotePath, **kwargs) -> bytes:
        with self.client.open(os.fspath(path), mode="rb", **kwargs) as file:
            return file.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="w", encoding=encoding, **kwargs) as file:
            file.write(content)

    def _write_bytes(self, path: RemotePath, content: bytes, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="wb", **kwargs) as file:
            file.write(content)
