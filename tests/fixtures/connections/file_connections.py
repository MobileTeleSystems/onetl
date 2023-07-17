import pytest
from pytest_lazyfixture import lazy_fixture


@pytest.fixture(
    params=[
        lazy_fixture("ftp_file_connection"),
        lazy_fixture("ftps_file_connection"),
        lazy_fixture("hdfs_file_connection"),
        lazy_fixture("s3_file_connection"),
        lazy_fixture("sftp_file_connection"),
        lazy_fixture("webdav_file_connection"),
    ],
)
def file_connection(request):
    return request.param


@pytest.fixture(
    params=[
        lazy_fixture("ftp_file_connection_with_path"),
        lazy_fixture("ftps_file_connection_with_path"),
        lazy_fixture("hdfs_file_connection_with_path"),
        lazy_fixture("s3_file_connection_with_path"),
        lazy_fixture("sftp_file_connection_with_path"),
        lazy_fixture("webdav_file_connection_with_path"),
    ],
)
def file_connection_with_path(request):
    return request.param


@pytest.fixture(
    params=[
        lazy_fixture("ftp_file_connection_with_path_and_files"),
        lazy_fixture("ftps_file_connection_with_path_and_files"),
        lazy_fixture("hdfs_file_connection_with_path_and_files"),
        lazy_fixture("s3_file_connection_with_path_and_files"),
        lazy_fixture("sftp_file_connection_with_path_and_files"),
        lazy_fixture("webdav_file_connection_with_path_and_files"),
    ],
)
def file_connection_with_path_and_files(request):
    return request.param
