import posixpath
from logging import getLogger
from dataclasses import dataclass
from typing import List, Union

from hdfs import InsecureClient, HdfsError
from hdfs.ext.kerberos import KerberosClient

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.kereberos_helpers import KerberosMixin

log = getLogger(__name__)


@dataclass(frozen=True)
class HDFS(FileConnection, KerberosMixin):
    port: int = 50070

    def get_client(self) -> Union["hdfs.ext.kerberos.KerberosClient", "hdfs.client.InsecureClient"]:
        conn_str = f"http://{self.host}:{self.port}"  # NOSONAR
        if self.extra.get("keytab") or self.password:
            self.kinit(
                self.user,
                keytab=self.extra.get("keytab"),
                password=self.password,
            )
            client = KerberosClient(conn_str, timeout=self.extra.get("timeout"))
        else:
            client = InsecureClient(conn_str, user=self.user)
        client.status("/")
        return client

    def download_file(self, remote_file_path: str, local_file_path: str) -> None:
        return self.client.download(remote_file_path, local_file_path)

    def remove_file(self, remote_file_path: str) -> None:
        if not self.path_exists(remote_file_path):
            raise HdfsError(f"{remote_file_path} doesn`t exists")
        self.client.delete(remote_file_path, recursive=True)
        log.info(f"Successfully removed file {remote_file_path}")

    def is_dir(self, top: str, item: str) -> bool:
        if self.client.status(posixpath.join(top, self.get_name(item)))["type"] == "DIRECTORY":
            return True

    def get_name(self, item: str) -> str:
        return item

    def path_exists(self, target_hdfs_path: str) -> bool:
        return self.client.status(target_hdfs_path, strict=False)

    def mkdir(self, path: str) -> None:
        self.client.makedirs(path)
        log.info(f"Successfully created directory {path}")

    def upload_file(self, local_file_path: str, remote_file_path: str, *args, **kwargs) -> None:
        self.client.upload(remote_file_path, local_file_path)

    def rename(self, source: str, target: str) -> None:
        self.client.rename(source, target)
        log.info(f"Successfully renamed file {source} to {target}")

    def rmdir(self, path: str, recursive: bool) -> None:
        self.client.delete(path, recursive=True)
        log.info(f"Successfully removed path {path}")

    def _listdir(self, path: str) -> List:
        return self.client.list(path)
