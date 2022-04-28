from __future__ import annotations

import os
from dataclasses import dataclass
from logging import getLogger

from hdfs import InsecureClient
from hdfs.ext.kerberos import KerberosClient

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.kerberos_helpers import kinit
from onetl.impl import RemoteFileStat

log = getLogger(__name__)


@dataclass(frozen=True)
class HDFS(FileConnection):
    """Class for HDFS file connection.

    Parameters
    ----------
    host : str
        Host of hdfs source. For example: ``rnd-dwh-nn-001.msk.mts.ru``
    port : int, default: ``50070``
        Port of hdfs source
    user : str
        User, which have access to the file source. For example: ``tech_etl``
    password : str, default: ``None``
        Password for file source connection

        .. warning ::

            To correct work you can provide only one of the parameters: ``password`` or ``kinit``.
            If you provide both, connection will raise Exception.
    kinit : str, default: ``None``
        Path to keytab file.

        .. warning ::

            To correct work you can provide only one of the parameters: ``password`` or ``kinit``.
            If you provide both, connection will raise Exception.
    timeout : int, default: ``10``
        Connection timeouts, forwarded to the request handler.
        How long to wait for the server to send data before giving up.

    Examples
    --------

    HDFS file connection initialization with password

    .. code::

        from onetl.connection import HDFS

        hdfs = HDFS(
            host="rnd-dwh-nn-001.msk.mts.ru",
            user="tech_etl",
            password="*****",
        )

    HDFS file connection initialization with keytab

    .. code::

        from onetl.connection import HDFS

        hdfs = HDFS(
            host="rnd-dwh-nn-001.msk.mts.ru",
            user="tech_etl",
            keytab="/path/to/keytab",
        )
    """

    port: int = 50070
    user: str = ""
    keytab: str | None = None  # TODO: change to Path
    timeout: int | None = None

    def __post_init__(self):
        if self.keytab and self.password:
            raise ValueError("Please provide only `keytab` or only `password` for kinit")

    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.status(path, strict=False)

    def _get_client(self) -> KerberosClient | InsecureClient:
        conn_str = f"http://{self.host}:{self.port}"  # NOSONAR
        if self.keytab or self.password:
            kinit(
                self.user,
                keytab=self.keytab,
                password=self.password,
            )
            client = KerberosClient(conn_str, timeout=self.timeout)
        else:
            client = InsecureClient(conn_str, user=self.user)

        return client

    def _rmdir_recursive(self, path: os.PathLike | str) -> None:
        self.client.delete(path, recursive=True)

    def _rmdir(self, path: os.PathLike | str) -> None:
        self.client.delete(path, recursive=False)

    def _mkdir(self, path: os.PathLike | str) -> None:
        self.client.makedirs(path)

    def _upload_file(self, local_file_path: os.PathLike | str, remote_file_path: os.PathLike | str) -> None:
        self.client.upload(remote_file_path, local_file_path)

    def _rename(self, source: os.PathLike | str, target: os.PathLike | str) -> None:
        self.client.rename(source, target)

    def _download_file(self, remote_file_path: os.PathLike | str, local_file_path: os.PathLike | str) -> None:
        self.client.download(remote_file_path, local_file_path)

    def _remove_file(self, remote_file_path: os.PathLike | str) -> None:
        self.client.delete(remote_file_path, recursive=False)

    def _listdir(self, path: os.PathLike | str) -> list:
        return self.client.list(path)

    def _is_file(self, path: os.PathLike | str) -> bool:
        return self.client.status(path)["type"] == "FILE"

    def _is_dir(self, path: os.PathLike | str) -> bool:
        return self.client.status(path)["type"] == "DIRECTORY"

    def _get_stat(self, path: os.PathLike | str) -> RemoteFileStat:
        stat = self.client.status(path)
        return RemoteFileStat(st_size=stat["length"], st_mtime=stat["modificationTime"])
