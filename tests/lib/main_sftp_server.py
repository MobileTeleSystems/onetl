import argparse
import os
import socket
import sys
import threading
import time

import paramiko
from sftpserver.stub_sftp import StubServer, StubSFTPServer

CERT_PEM_FILE = os.path.join(os.path.dirname(__file__), "../resources/keycert.pem")

HOST, PORT = "localhost", 3373
BACKLOG = 10


class StubSFTPServerExt(StubSFTPServer):
    def __init__(self, homedir, server, *args, **kwargs):
        super().__init__(server, *args, **kwargs)
        self.ROOT = homedir


class ConnHandlerThd(threading.Thread):
    def __init__(self, conn, keyfile, homedir):
        super().__init__()
        self._conn = conn
        self._keyfile = keyfile
        self._homedir = homedir

    def run(self):
        host_key = paramiko.RSAKey.from_private_key_file(self._keyfile)
        transport = paramiko.Transport(self._conn)
        transport.add_server_key(host_key)
        transport.set_subsystem_handler("sftp", paramiko.SFTPServer, StubSFTPServer)

        server = StubServer()
        transport.start_server(server=server)

        channel = transport.accept()  # noqa: F841
        while transport.is_active():
            time.sleep(1)


def start_server(host, port, homedir, keyfile, level):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)  # noqa: WPS425
    server_socket.bind((host, port))
    server_socket.listen(BACKLOG)

    while True:  # noqa: WPS457
        conn, addr = server_socket.accept()

        srv_thd = ConnHandlerThd(conn, keyfile, homedir)
        srv_thd.setDaemon(True)
        srv_thd.start()


def create_sftp_server_process(ftp_host, ftp_port, ftp_homedir):
    start_server(ftp_host, ftp_port, ftp_homedir, CERT_PEM_FILE, "INFO")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--host", action="store")
    parser.add_argument("--port", action="store")
    parser.add_argument("--homedir", action="store")
    args = parser.parse_args(sys.argv[1:])
    create_sftp_server_process(args.host, int(args.port), args.homedir)
