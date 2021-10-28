import os
import re
import shutil
import time
from subprocess import Popen

import paramiko


CLIENT_CONNECTION_RETRY_DELAY_SEC = 1
CLIENT_CONNECTION_RETRIES = 5


def find_py(name, path):
    for dir_path, _dir_names, filenames in os.walk(path):
        for files in filenames:
            regexp = f".*{name}.*py"
            if re.match(regexp, files):
                return os.path.join(dir_path, files)
    raise RuntimeError(f"Couldn`t find test ftp server py in directory {path}")


class MockSFtpServer:
    def __init__(self, homedir):
        self.host = "127.0.0.1"
        self.port = 2222
        self.user = "user1"
        self.password = "passwd"
        self.homedir = homedir
        self.server = None

    def start(self):
        if not self.server:
            if not os.path.exists(self.homedir):
                os.mkdir(self.homedir)

            test_sftp_server_py = find_py("main_sftp_server", os.path.dirname(os.path.abspath(__file__)))
            cmd = [  # noqa: WPS317
                "python3",
                test_sftp_server_py,
                "--host",
                self.host,
                "--port",
                str(self.port),
                "--homedir",
                self.homedir,
            ]
            current_path = os.getcwd()
            os.chdir(self.homedir)
            self.server = Popen(cmd, bufsize=0, universal_newlines=True)
            os.chdir(current_path)

    def clear(self):
        if os.path.exists(self.homedir):
            shutil.rmtree(self.homedir)

    def stop(self):
        self.server.terminate()
        self.server.wait()
        self.clear()

    def create_client(self):
        retries = 0
        max_retries = CLIENT_CONNECTION_RETRIES
        while retries < max_retries:
            try:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(self.host, self.port, self.user, self.password)
                sftp_client = ssh_client.open_sftp()
                return ssh_client, sftp_client
            except Exception:
                retries += 1
                time.sleep(CLIENT_CONNECTION_RETRY_DELAY_SEC)
        if retries == max_retries:
            raise RuntimeError(f"Couldn`t connect to local FTP server after {retries} retries")
