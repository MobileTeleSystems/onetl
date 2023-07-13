import secrets
import shutil
from pathlib import Path

import pytest
from pytest_lazyfixture import lazy_fixture


@pytest.fixture(scope="session")
def resource_path_original():
    path = Path(__file__).parent.parent.parent / "resources" / "src"
    assert path.exists()
    return path


@pytest.fixture()
def resource_path(resource_path_original, tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("test_files") / secrets.token_hex(5)
    shutil.copytree(resource_path_original, temp_dir)
    return temp_dir


@pytest.fixture()
def test_files(resource_path):
    resources = resource_path / "news_parse_zp" / "2018_03_05_10_00_00"
    return [
        resources / "newsage-zp-2018_03_05_10_00_00.csv",
        resources / "newsage-zp-2018_03_05_10_10_00.csv",
    ]


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
