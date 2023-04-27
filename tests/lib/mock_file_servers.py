import os
import re
import shutil
from subprocess import Popen

CLIENT_CONNECTION_RETRY_DELAY_SEC = 1
CLIENT_CONNECTION_RETRIES = 5
LOCAL_HOST = "127.0.0.1"


def find_py_extention(name, path):
    for dir_path, _dir_names, filenames in os.walk(path):
        for files in filenames:
            regexp = rf".*{name}.*\.py"
            if re.match(regexp, files):
                return os.path.join(dir_path, files)
    raise RuntimeError(f"Couldn`t find test ftp server py in directory {path}")


class TestFTPServer:
    def __init__(self, homedir, is_ftps=False):
        self.host = LOCAL_HOST
        self.ftps = is_ftps
        if is_ftps:
            self.port = int(os.getenv("ONETL_FTPS_PORT"))
        else:
            self.port = int(os.getenv("ONETL_FTP_PORT"))
        self.user = os.getenv("ONETL_FILE_SOURCE_USER")
        self.password = os.getenv("ONETL_FILE_SOURCE_PASSWORD")
        self.homedir = homedir
        self.server = None

    def start(self):
        if self.server:
            pass
        else:
            if not os.path.exists(self.homedir):
                os.mkdir(self.homedir)

            test_ftp_server_py = find_py_extention("main_ftp_server", os.path.dirname(os.path.abspath(__file__)))
            cmd = [
                "python3",
                test_ftp_server_py,
                "--host",
                self.host,
                "--port",
                str(self.port),
                "--homedir",
                self.homedir,
                "--user",
                self.user,
                "--password",
                self.password,
            ]
            if self.ftps:
                cmd.extend(["--ftps", "true"])

            self.server = Popen(cmd, bufsize=0, universal_newlines=True)

    def clear(self):
        if os.path.exists(self.homedir):
            shutil.rmtree(self.homedir)

    def stop(self):
        self.server.terminate()
        self.server.wait()
        self.clear()
