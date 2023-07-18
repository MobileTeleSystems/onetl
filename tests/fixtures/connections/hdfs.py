import os
from collections import namedtuple
from pathlib import PurePosixPath

import pytest

from tests.util.upload_files import upload_files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("real", marks=[pytest.mark.hdfs, pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def hdfs_server():
    HDFSServer = namedtuple("HDFSServer", ["host", "port"])
    return HDFSServer(
        host=os.getenv("ONETL_HDFS_HOST"),
        port=os.getenv("ONETL_HDFS_PORT"),
    )


@pytest.fixture()
def hdfs_file_connection(hdfs_server):
    from onetl.connection import HDFS

    return HDFS(host=hdfs_server.host, port=hdfs_server.port)


@pytest.fixture()
def hdfs_file_connection_with_path(request, hdfs_file_connection):
    connection = hdfs_file_connection
    root = PurePosixPath("/data/")

    def finalizer():
        connection.remove_dir(root, recursive=True)

    request.addfinalizer(finalizer)

    connection.remove_dir(root, recursive=True)
    connection.create_dir(root)

    return connection, root


@pytest.fixture()
def hdfs_file_connection_with_path_and_files(resource_path_original, hdfs_file_connection_with_path):
    connection, upload_to = hdfs_file_connection_with_path
    upload_from = resource_path_original
    files = upload_files(upload_from, upload_to, connection)
    return connection, upload_to, files
