import argparse
import os
import sys

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

CERT_PEM_FILE = os.path.join(os.path.dirname(__file__), "../resources/keycert.pem")


def create_ftp_server_process(ftp_host, ftp_port, ftp_homedir, ftp_user, ftp_password, ftps=False):
    authorizer = DummyAuthorizer()
    authorizer.add_user(ftp_user, ftp_password, homedir=ftp_homedir, perm="elradfmwM")
    authorizer.add_anonymous(os.getcwd())
    handler = FTPHandler
    if ftps:
        from pyftpdlib.handlers import TLS_FTPHandler

        handler = TLS_FTPHandler
        handler.certfile = CERT_PEM_FILE
    handler.authorizer = authorizer
    ftp_server = FTPServer((ftp_host, ftp_port), handler)
    ftp_server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--host", action="store")
    parser.add_argument("--port", action="store")
    parser.add_argument("--homedir", action="store")
    parser.add_argument("--user", action="store")
    parser.add_argument("--password", action="store")
    parser.add_argument("--ftps", action="store")
    args = parser.parse_args(sys.argv[1:])
    create_ftp_server_process(args.host, args.port, args.homedir, args.user, args.password, bool(args.ftps))
