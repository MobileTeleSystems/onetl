import posixpath
from dataclasses import dataclass

from hdfs import InsecureClient, HdfsError
from hdfs.ext.kerberos import KerberosClient

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.kereberos_helpers import KerberosMixin


@dataclass(frozen=True)
class HDFS(FileConnection, KerberosMixin):
    port: int = 50070

    def get_client(self):
        conn_str = f'http://{self.host}:{self.port}'
        if self.extra.get('keytab') or self.password:
            self.kinit(
                self.login,
                keytab=self.extra.get('keytab'),
                password=self.password,
            )
            client = KerberosClient(conn_str, timeout=self.extra.get('timeout'))
        else:
            client = InsecureClient(conn_str, user=self.login)
        client.status('/')
        return client

    def download_file(self, remote_file_path: str, local_file_path: str) -> None:
        return self.client.download(remote_file_path, local_file_path)

    def remove_file(self, remote_file_path: str):
        if not self.path_exists(remote_file_path):
            raise HdfsError(f'{remote_file_path} doesn`t exists')
        self.client.delete(remote_file_path, recursive=True)

    def is_dir(self, top, item):
        if self.client.status(posixpath.join(top, self.get_name(item)))['type'] == 'DIRECTORY':
            return True

    def get_name(self, item):
        return item

    def path_exists(self, target_hdfs_path):
        return self.client.status(target_hdfs_path, strict=False)

    def _listdir(self, path):
        return self.client.list(path)

    # TODO: Перенести другие методы
