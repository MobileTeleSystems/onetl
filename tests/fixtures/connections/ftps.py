import os
from collections import namedtuple
from pathlib import PurePosixPath

import pytest

from tests.util.upload_files import upload_files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("real-ftps", marks=[pytest.mark.ftps, pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def ftps_server():
    FTPSServer = namedtuple("FTPSServer", ["host", "port", "user", "password"])

    return FTPSServer(
        host=os.getenv("ONETL_FTPS_HOST"),
        port=os.getenv("ONETL_FTPS_PORT"),
        user=os.getenv("ONETL_FTPS_USER"),
        password=os.getenv("ONETL_FTPS_PASSWORD"),
    )


@pytest.fixture()
def ftps_file_connection(ftps_server):
    from onetl.connection import FTPS

    return FTPS(
        host=ftps_server.host,
        port=ftps_server.port,
        user=ftps_server.user,
        password=ftps_server.password,
    )


@pytest.fixture()
def ftps_file_connection_with_path(request, ftps_file_connection):
    connection = ftps_file_connection
    root = PurePosixPath("/data")

    def finalizer():
        connection.remove_dir(root, recursive=True)

    request.addfinalizer(finalizer)

    connection.remove_dir(root, recursive=True)
    connection.create_dir(root)

    return connection, root


@pytest.fixture()
def ftps_file_connection_with_path_and_files(resource_path, ftps_file_connection_with_path):
    connection, upload_to = ftps_file_connection_with_path
    upload_from = resource_path / "file_connection"
    files = upload_files(upload_from, upload_to, connection)
    return connection, upload_to, files
