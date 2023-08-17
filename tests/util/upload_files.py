from __future__ import annotations

import logging
import os

from onetl.connection import FileConnection
from onetl.impl import LocalPath, RemotePath

log = logging.getLogger(__name__)


def upload_files(
    source_path: os.PathLike | str,
    remote_path: os.PathLike | str,
    file_connection: FileConnection,
) -> list[RemotePath]:
    remote_files = []

    local_path = LocalPath(source_path)

    if local_path.exists() and local_path.is_dir():
        for root_path, _dir_names, file_names in os.walk(local_path):
            local_root = LocalPath(root_path)
            remote_root = RemotePath(remote_path) / local_root.relative_to(local_path)

            for filename in file_names:
                local_filename = local_root / filename
                remote_filename = remote_root / filename
                file_connection.upload_file(local_filename, remote_filename)
                remote_files.append(remote_filename)

    if not remote_files:
        raise RuntimeError(
            f"Could not load file examples from {local_path}. Path should exist and should contain samples",
        )

    return remote_files
