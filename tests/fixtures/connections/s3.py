import os
from collections import namedtuple
from pathlib import PurePosixPath

import pytest

from tests.fixtures.connections.util import upload_files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("real", marks=[pytest.mark.s3, pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def s3_server():
    S3Server = namedtuple("S3Server", ["host", "port", "bucket", "access_key", "secret_key", "protocol"])

    return S3Server(
        host=os.getenv("ONETL_S3_HOST"),
        port=os.getenv("ONETL_S3_PORT"),
        bucket=os.getenv("ONETL_S3_BUCKET"),
        access_key=os.getenv("ONETL_S3_ACCESS_KEY"),
        secret_key=os.getenv("ONETL_S3_SECRET_KEY"),
        protocol=os.getenv("ONETL_S3_PROTOCOL", "http").lower(),
    )


@pytest.fixture()
def s3_file_connection(s3_server):
    from onetl.connection import S3

    return S3(
        host=s3_server.host,
        port=s3_server.port,
        bucket=s3_server.bucket,
        access_key=s3_server.access_key,
        secret_key=s3_server.secret_key,
        protocol=s3_server.protocol,
    )


@pytest.fixture()
def s3_file_connection_with_path(request, s3_file_connection):
    connection = s3_file_connection
    root = PurePosixPath("/data/")

    if not connection.client.bucket_exists(connection.bucket):
        connection.client.make_bucket(connection.bucket)

    def finalizer():
        connection.remove_dir(root, recursive=True)

    request.addfinalizer(finalizer)

    connection.remove_dir(root, recursive=True)
    connection.create_dir(root)

    return connection, root


@pytest.fixture()
def s3_file_connection_with_path_and_files(resource_path_original, s3_file_connection_with_path):
    connection, upload_to = s3_file_connection_with_path
    upload_from = resource_path_original
    files = upload_files(upload_from, upload_to, connection)
    return connection, upload_to, files
