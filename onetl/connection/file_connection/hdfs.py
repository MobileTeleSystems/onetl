from __future__ import annotations

import os
import stat
from logging import getLogger
from typing import Optional

from hdfs import InsecureClient
from hdfs.ext.kerberos import KerberosClient
from pydantic import FilePath, SecretStr, root_validator

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.kerberos_helpers import kinit
from onetl.impl import LocalPath, RemotePath, RemotePathStat

log = getLogger(__name__)


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
    keytab : str, default: ``None``
        LocalPath to keytab file.

        .. warning ::

            To correct work you can provide only one of the parameters: ``password`` or ``kinit``.
            If you provide both, connection will raise Exception.
    timeout : int, default: ``10``
        Connection timeouts, forwarded to the request handler.
        How long to wait for the server to send data before giving up.

    Examples
    --------

    HDFS file connection initialization with password

    .. code:: python

        from onetl.connection import HDFS

        hdfs = HDFS(
            host="rnd-dwh-nn-001.msk.mts.ru",
            user="tech_etl",
            password="*****",
        )

    HDFS file connection initialization with keytab

    .. code:: python

        from onetl.connection import HDFS

        hdfs = HDFS(
            host="rnd-dwh-nn-001.msk.mts.ru",
            user="tech_etl",
            keytab="/path/to/keytab",
        )
    """

    host: str
    port: int = 50070
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    keytab: Optional[FilePath] = None
    timeout: int = 10

    @root_validator
    def check_credentials(cls, values):  # noqa: N805
        user = values.get("user")
        password = values.get("password")
        keytab = values.get("keytab")
        if password and keytab:
            raise ValueError("Please provide either `keytab` or `password` for kinit, not both")

        if (password or keytab) and not user:
            raise ValueError("`keytab` or `password` should be used only with `user`")

        return values

    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.status(os.fspath(path), strict=False)

    def _get_client(self) -> KerberosClient | InsecureClient:
        conn_str = f"http://{self.host}:{self.port}"  # NOSONAR
        if self.user and (self.keytab or self.password):
            kinit(
                self.user,
                keytab=self.keytab,
                password=self.password.get_secret_value() if self.password else None,
            )
            client = KerberosClient(conn_str, timeout=self.timeout)
        else:
            client = InsecureClient(conn_str, user=self.user)

        return client

    def _is_client_closed(self) -> bool:
        # Underlying client does not support closing
        return False

    def _close_client(self) -> None:  # NOSONAR
        # Underlying client does not support closing
        pass  # noqa: WPS420

    def _rmdir(self, path: RemotePath) -> None:
        self.client.delete(os.fspath(path), recursive=False)

    def _mkdir(self, path: RemotePath) -> None:
        self.client.makedirs(os.fspath(path))

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        self.client.upload(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _rename(self, source: RemotePath, target: RemotePath) -> None:
        self.client.rename(os.fspath(source), os.fspath(target))

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        self.client.download(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.delete(os.fspath(remote_file_path), recursive=False)

    def _listdir(self, path: RemotePath) -> list:
        return self.client.list(os.fspath(path))

    def _is_file(self, path: RemotePath) -> bool:
        return self.client.status(os.fspath(path))["type"] == "FILE"

    def _is_dir(self, path: RemotePath) -> bool:
        return self.client.status(os.fspath(path))["type"] == "DIRECTORY"

    def _get_stat(self, path: RemotePath) -> RemotePathStat:
        status = self.client.status(os.fspath(path))

        # Status examples:
        # {
        #   "accessTime"      : 1320171722771,
        #   "blockSize"       : 33554432,
        #   "group"           : "supergroup",
        #   "length"          : 24930,
        #   "modificationTime": 1320171722771,
        #   "owner"           : "webuser",
        #   "pathSuffix"      : "a.patch",
        #   "permission"      : "644",
        #   "replication"     : 1,
        #   "type"            : "FILE"
        # }
        #
        # {
        #   "accessTime"      : 0,
        #   "blockSize"       : 0,
        #   "group"           : "supergroup",
        #   "length"          : 0,
        #   "modificationTime": 1320895981256,
        #   "owner"           : "szetszwo",
        #   "pathSuffix"      : "bar",
        #   "permission"      : "711",
        #   "replication"     : 0,
        #   "type"            : "DIRECTORY"
        # }

        path_type = stat.S_IFDIR if status["type"] == "DIRECTORY" else stat.S_IFREG

        return RemotePathStat(
            st_size=status["length"],
            st_mtime=status["modificationTime"] / 1000,  # HDFS uses timestamps with milliseconds
            st_uid=status["owner"],
            st_gid=status["group"],
            st_mode=int(status["permission"], 8) | path_type,
        )

    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        with self.client.read(os.fspath(path), encoding=encoding, **kwargs) as file:
            return file.read()

    def _read_bytes(self, path: RemotePath, **kwargs) -> bytes:
        with self.client.read(os.fspath(path), **kwargs) as file:
            return file.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str, **kwargs) -> None:
        if not isinstance(content, str):
            raise TypeError(f"content must be str, not '{content.__class__.__name__}'")
        self.client.write(os.fspath(path), data=content, encoding=encoding, overwrite=True, **kwargs)

    def _write_bytes(self, path: RemotePath, content: bytes, **kwargs) -> None:
        if not isinstance(content, bytes):
            raise TypeError(f"content must be bytes, not '{content.__class__.__name__}'")
        self.client.write(os.fspath(path), data=content, overwrite=True, **kwargs)
