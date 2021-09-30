import os
from dataclasses import dataclass
from logging import getLogger
from stat import S_ISDIR

import paramiko

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


@dataclass(frozen=True)
class SSH(FileConnection):
    port: int = 22

    def get_client(self):  # noqa: WPS231
        log.debug('creating ssh client')

        key_file = self.extra.get('key_file')
        timeout = int(self.extra.get('timeout'), 10)
        compress = True
        no_host_key_check = self.extra.get('no_host_key_check', True)

        if self.extra.get('compress') and self.extra['compress'].lower() == 'false':
            compress = False

        host_proxy = None
        user_ssh_config_filename = os.path.expanduser('~/.ssh/config')
        if os.path.isfile(user_ssh_config_filename):
            ssh_conf = paramiko.SSHConfig()
            ssh_conf.parse(open(user_ssh_config_filename)) # NOQA WPS515
            host_info = ssh_conf.lookup(self.host)
            host_proxy = None
            if host_info and host_info.get('proxycommand'):
                host_proxy = paramiko.ProxyCommand(host_info.get('proxycommand'))

            if not (self.password or key_file):
                if host_info and host_info.get('identityfile'):
                    key_file = host_info.get('identityfile')[0] # NOQA WPS220

        try:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            if no_host_key_check:
                # Default is RejectPolicy
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if self.password and self.password.strip():
                client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password,
                    timeout=timeout,
                    compress=compress,
                    sock=host_proxy,
                )
            else:
                client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.user,
                    key_filename=key_file,
                    timeout=timeout,
                    compress=compress,
                    sock=host_proxy,
                )

        except paramiko.AuthenticationException as auth_error:
            raise paramiko.AuthenticationException(
                f'Auth failed while connecting to host: {self.host}, error: {auth_error}',
            )
        except paramiko.SSHException as ssh_error:
            raise paramiko.SSHException(
                f'Failed connecting to host: {self.host}, error: {ssh_error}',
            )
        except Exception as error:
            raise RuntimeError(
                f'Error connecting to host: {self.host}, error: {error}',
            )

        return client.open_sftp()

    def is_dir(self, top, item):
        return S_ISDIR(item.st_mode)

    def get_name(self, item):
        return item.filename

    def download_file(self, remote_file_path, local_file_path):
        self.client.get(remote_file_path, local_file_path)
        log.info(f'Successfully download _file {remote_file_path} remote SFTP to {local_file_path}')

    def remove_file(self, remote_file_path):
        self.client.remove(remote_file_path)
        log.info(f'Successfully removed file {remote_file_path}')

    def path_exists(self, path):
        try:
            self.client.stat(path)
            return True
        except FileNotFoundError:
            return False

    def _listdir(self, path):
        return self.client.listdir_attr(path)
