from __future__ import annotations

import ftplib  # noqa: S402
from pathlib import PosixPath
import os
from logging import getLogger
from dataclasses import dataclass

from ftputil import FTPHost, session as ftp_session

from onetl.connection.file_connection.file_connection import FileConnection

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

        from onetl.connection.file_connection import FTP

        ftp = FTP(
            host="0001testadviat04.msk.mts.ru",
            user="sa0000sphrsftptest",
            password="*****",
        )
    """

    port: int = 21

    def get_client(self) -> ftputil.host.FTPHost:
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

    def is_dir(self, top: os.PathLike | str, item: str | os.PathLike) -> bool:
        return self.client.path.isdir(top / self.get_name(item))

    def get_name(self, item: str | os.PathLike) -> PosixPath:
        return PosixPath(item)

    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.stat(path=path, _exception_for_missing_path=False)

    def rmdir(self, path: os.PathLike | str, recursive: bool = False) -> None:
        if recursive:
            self.client.rmtree(path)
        else:
            self.client.rmdir(path)
        log.info(f"|{self.__class__.__name__}| Successfully removed directory {path}")

    def _upload_file(self, local_file_path: os.PathLike | str, remote_file_path: os.PathLike | str) -> None:
        self.client.upload(local_file_path, remote_file_path)

    def _rename(self, source: os.PathLike | str, target: os.PathLike | str) -> None:
        self.client.rename(source, target)

    def _download_file(self, remote_file_path: os.PathLike | str, local_file_path: os.PathLike | str) -> None:
        self.client.download(remote_file_path, local_file_path)

    def _remove_file(self, remote_file_path: os.PathLike | str) -> None:
        self.client.remove(remote_file_path)

    def _mkdir(self, path: os.PathLike | str) -> None:
        self.client.makedirs(path, exist_ok=True)

    def _listdir(self, path: os.PathLike | str) -> list:
        return self.client.listdir(path)
