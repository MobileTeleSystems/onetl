import os
from collections import namedtuple
from pathlib import PurePosixPath

import pytest

from tests.util.upload_files import upload_files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("real-s3", marks=[pytest.mark.s3, pytest.mark.connection]),
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


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("s3-file", marks=[pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def s3_file_connection(s3_server):
    from onetl.connection import S3

    s3 = S3(
        host=s3_server.host,
        port=s3_server.port,
        bucket=s3_server.bucket,
        access_key=s3_server.access_key,
        secret_key=s3_server.secret_key,
        protocol=s3_server.protocol,
    )

    if not s3.client.bucket_exists(s3_server.bucket):
        s3.client.make_bucket(s3_server.bucket)

    return s3


@pytest.fixture()
def s3_file_connection_with_path(request, s3_file_connection):
    connection = s3_file_connection
    root = PurePosixPath("/data")

    def finalizer():
        connection.remove_dir(root, recursive=True)

    request.addfinalizer(finalizer)

    connection.remove_dir(root, recursive=True)

    return connection, root


@pytest.fixture()
def s3_file_connection_with_path_and_files(resource_path, s3_file_connection_with_path):
    connection, upload_to = s3_file_connection_with_path
    upload_from = resource_path / "file_connection"
    files = upload_files(upload_from, upload_to, connection)
    return connection, upload_to, files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("s3-file-df", marks=[pytest.mark.file_df_connection, pytest.mark.connection]),
    ],
)
def s3_file_df_connection(s3_file_connection, spark, s3_server):
    from onetl.connection import SparkS3

    # s3_file_connection is used only to create bucket
    return SparkS3(
        host=s3_server.host,
        port=s3_server.port,
        bucket=s3_server.bucket,
        access_key=s3_server.access_key,
        secret_key=s3_server.secret_key,
        protocol=s3_server.protocol,
        extra={
            "path.style.access": True,
        },
        spark=spark,
    )


@pytest.fixture()
def s3_file_df_connection_with_path(s3_file_connection_with_path, s3_file_df_connection):
    _, root = s3_file_connection_with_path
    return s3_file_df_connection, root


@pytest.fixture()
def s3_file_df_connection_with_path_and_files(resource_path, s3_file_connection, s3_file_df_connection_with_path):
    connection, upload_to = s3_file_df_connection_with_path
    upload_from = resource_path / "file_df_connection"
    files = upload_files(upload_from, upload_to, s3_file_connection)
    return connection, upload_to, files
