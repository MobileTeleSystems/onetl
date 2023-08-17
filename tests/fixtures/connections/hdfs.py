import os
from collections import namedtuple
from pathlib import PurePosixPath

import pytest

from tests.util.upload_files import upload_files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("real-hdfs", marks=[pytest.mark.hdfs, pytest.mark.connection]),
    ],
)
def hdfs_server():
    HDFSServer = namedtuple("HDFSServer", ["host", "webhdfs_port", "ipc_port"])
    return HDFSServer(
        host=os.getenv("ONETL_HDFS_HOST"),
        webhdfs_port=os.getenv("ONETL_HDFS_WEBHDFS_PORT"),
        ipc_port=os.getenv("ONETL_HDFS_IPC_PORT"),
    )


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("hdfs-file", marks=[pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def hdfs_file_connection(hdfs_server):
    from onetl.connection import HDFS

    return HDFS(host=hdfs_server.host, webhdfs_port=hdfs_server.webhdfs_port)


@pytest.fixture()
def hdfs_file_connection_with_path(request, hdfs_file_connection):
    connection = hdfs_file_connection
    root = PurePosixPath("/data")

    def finalizer():
        connection.remove_dir(root, recursive=True)

    request.addfinalizer(finalizer)

    connection.remove_dir(root, recursive=True)
    connection.create_dir(root)

    return connection, root


@pytest.fixture()
def hdfs_file_connection_with_path_and_files(resource_path, hdfs_file_connection_with_path):
    connection, upload_to = hdfs_file_connection_with_path
    upload_from = resource_path / "file_connection"
    files = upload_files(upload_from, upload_to, connection)
    return connection, upload_to, files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("hdfs-file-df", marks=[pytest.mark.file_df_connection, pytest.mark.connection]),
    ],
)
def hdfs_file_df_connection(spark, hdfs_server):
    from onetl.connection import SparkHDFS

    return SparkHDFS(
        cluster="rnd-dwh",
        host=hdfs_server.host,
        ipc_port=hdfs_server.ipc_port,
        spark=spark,
    )


@pytest.fixture()
def hdfs_file_df_connection_with_path(hdfs_file_connection_with_path, hdfs_file_df_connection):
    _, root = hdfs_file_connection_with_path
    return hdfs_file_df_connection, root


@pytest.fixture()
def hdfs_file_df_connection_with_path_and_files(resource_path, hdfs_file_connection, hdfs_file_df_connection_with_path):
    connection, upload_to = hdfs_file_df_connection_with_path
    upload_from = resource_path / "file_df_connection"
    files = upload_files(upload_from, upload_to, hdfs_file_connection)
    return connection, upload_to, files
