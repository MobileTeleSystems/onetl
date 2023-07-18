import os
from collections import namedtuple
from pathlib import PurePosixPath

import pytest

from tests.util.upload_files import upload_files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(
            "real-webdav",
            marks=[pytest.mark.webdav, pytest.mark.file_connection, pytest.mark.connection],
        ),
    ],
)
def webdav_server():
    WebDAVServer = namedtuple("WebDAVServer", ["host", "port", "user", "password", "ssl_verify", "protocol"])

    return WebDAVServer(
        host=os.getenv("ONETL_WEBDAV_HOST"),
        port=os.getenv("ONETL_WEBDAV_PORT"),
        user=os.getenv("ONETL_WEBDAV_USER"),
        password=os.getenv("ONETL_WEBDAV_PASSWORD"),
        ssl_verify=os.getenv("ONETL_WEBDAV_SSL_VERIFY", "false").lower() != "true",
        protocol=os.getenv("ONETL_WEBDAV_PROTOCOL", "http").lower(),
    )


@pytest.fixture()
def webdav_file_connection(webdav_server):
    from onetl.connection import WebDAV

    return WebDAV(
        host=webdav_server.host,
        port=webdav_server.port,
        user=webdav_server.user,
        password=webdav_server.password,
        ssl_verify=webdav_server.ssl_verify,
        protocol=webdav_server.protocol,
    )


@pytest.fixture()
def webdav_file_connection_with_path(request, webdav_file_connection):
    connection = webdav_file_connection
    root = PurePosixPath("/data")

    def finalizer():
        connection.remove_dir(root, recursive=True)

    request.addfinalizer(finalizer)

    connection.remove_dir(root, recursive=True)
    connection.create_dir(root)

    return connection, root


@pytest.fixture()
def webdav_file_connection_with_path_and_files(resource_path, webdav_file_connection_with_path):
    connection, upload_to = webdav_file_connection_with_path
    upload_from = resource_path / "file_connection"
    files = upload_files(upload_from, upload_to, connection)
    return connection, upload_to, files
