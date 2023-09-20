import os
from collections import namedtuple
from pathlib import PurePosixPath

import pytest

from tests.util.upload_files import upload_files


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("real-samba", marks=[pytest.mark.samba, pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def samba_server():
    SambaServer = namedtuple("SambaServer", ["host", "protocol", "port", "share", "user", "password"])

    return SambaServer(
        host=os.getenv("ONETL_SAMBA_HOST"),
        protocol=os.getenv("ONETL_SAMBA_PROTOCOL"),
        port=os.getenv("ONETL_SAMBA_PORT"),
        share=os.getenv("ONETL_SAMBA_SHARE"),
        user=os.getenv("ONETL_SAMBA_USER"),
        password=os.getenv("ONETL_SAMBA_PASSWORD"),
    )


@pytest.fixture()
def samba_file_connection(samba_server):
    from onetl.connection import Samba

    return Samba(
        host=samba_server.host,
        protocol=samba_server.protocol,
        port=samba_server.port,
        share=samba_server.share,
        user=samba_server.user,
        password=samba_server.password,
    )


@pytest.fixture()
def samba_file_connection_with_path(request, samba_file_connection):
    connection = samba_file_connection
    root = PurePosixPath("/data")

    def finalizer():
        connection.remove_dir(root, recursive=True)

    request.addfinalizer(finalizer)

    connection.remove_dir(root, recursive=True)

    return connection, root


@pytest.fixture()
def samba_file_connection_with_path_and_files(resource_path, samba_file_connection_with_path):
    connection, upload_to = samba_file_connection_with_path
    upload_from = resource_path / "file_connection"
    files = upload_files(upload_from, upload_to, connection)
    return connection, upload_to, files
