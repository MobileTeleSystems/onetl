import os
import shutil
from pathlib import Path

import pytest


@pytest.fixture(
    params=[
        pytest.param(
            "real-local-fs",
            marks=[pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection],
        ),
    ],
)
def local_fs_file_df_connection(spark):
    from onetl.connection import SparkLocalFS

    return SparkLocalFS(spark=spark)


@pytest.fixture()
def local_fs_file_df_connection_with_path(local_fs_file_df_connection, tmp_path_factory, worker_id):
    connection = local_fs_file_df_connection
    root = tmp_path_factory.mktemp("local-fs") / worker_id
    # Iceberg warehouse dir should be created beforehand
    root.mkdir(exist_ok=True)
    yield connection, root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture()
def local_fs_file_df_connection_with_path_and_files(
    local_fs_file_df_connection,
    tmp_path_factory,
    worker_id,
    resource_path,
):
    connection = local_fs_file_df_connection
    root = tmp_path_factory.mktemp("local-fs") / worker_id
    copy_from = resource_path / "file_df_connection"

    shutil.copytree(copy_from, root)

    files = []
    for directory, _, content in os.walk(root):
        for file in content:
            files.append(Path(directory) / file)

    yield connection, root, files
    shutil.rmtree(root, ignore_errors=True)
