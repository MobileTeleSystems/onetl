import os
from collections import namedtuple
from pathlib import PurePosixPath

import pytest

from tests.util.upload_files import upload_files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("real-sftp", marks=[pytest.mark.sftp, pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def sftp_server():
    SFTPServer = namedtuple("SFTPServer", ["host", "port", "user", "password"])

    return SFTPServer(
        host=os.getenv("ONETL_SFTP_HOST"),
        port=os.getenv("ONETL_SFTP_PORT"),
        user=os.getenv("ONETL_SFTP_USER"),
        password=os.getenv("ONETL_SFTP_PASSWORD"),
    )


@pytest.fixture()
def sftp_file_connection(sftp_server):
    from onetl.connection import SFTP

    return SFTP(
        host=sftp_server.host,
        port=sftp_server.port,
        user=sftp_server.user,
        password=sftp_server.password,
    )


@pytest.fixture()
def sftp_file_connection_with_path(request, sftp_file_connection):
    connection = sftp_file_connection
    root = PurePosixPath("/app/data")

    def finalizer():
        connection.remove_dir(root, recursive=True)

    request.addfinalizer(finalizer)

    connection.remove_dir(root, recursive=True)
    connection.create_dir(root)

    return connection, root


@pytest.fixture()
def sftp_file_connection_with_path_and_files(resource_path, sftp_file_connection_with_path):
    connection, upload_to = sftp_file_connection_with_path
    upload_from = resource_path / "file_connection"
    files = upload_files(upload_from, upload_to, connection)
    return connection, upload_to, files
