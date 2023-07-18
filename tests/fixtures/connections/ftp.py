import os
from collections import namedtuple
from pathlib import PurePosixPath

import pytest

from tests.util.upload_files import upload_files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("real-ftp", marks=[pytest.mark.ftp, pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def ftp_server():
    FTPServer = namedtuple("FTPServer", ["host", "port", "user", "password"])

    return FTPServer(
        host=os.getenv("ONETL_FTP_HOST"),
        port=os.getenv("ONETL_FTP_PORT"),
        user=os.getenv("ONETL_FTP_USER"),
        password=os.getenv("ONETL_FTP_PASSWORD"),
    )


@pytest.fixture()
def ftp_file_connection(ftp_server):
    from onetl.connection import FTP

    return FTP(
        host=ftp_server.host,
        port=ftp_server.port,
        user=ftp_server.user,
        password=ftp_server.password,
    )


@pytest.fixture()
def ftp_file_connection_with_path(request, ftp_file_connection):
    connection = ftp_file_connection
    root = PurePosixPath("/data")

    def finalizer():
        connection.remove_dir(root, recursive=True)

    request.addfinalizer(finalizer)

    connection.remove_dir(root, recursive=True)
    connection.create_dir(root)

    return connection, root


@pytest.fixture()
def ftp_file_connection_with_path_and_files(resource_path, ftp_file_connection_with_path):
    connection, upload_to = ftp_file_connection_with_path
    upload_from = resource_path / "file_connection"
    files = upload_files(upload_from, upload_to, connection)
    return connection, upload_to, files
