from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path, PurePosixPath

from onetl.connection.file_connection.file_connection import FileConnection

log = logging.getLogger(__name__)


def hashfile(file):
    buf_size = 65536
    sha256 = hashlib.sha256()

    with open(file, "rb") as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break

            sha256.update(data)

    return sha256.hexdigest()


def upload_files(
    source_path: os.PathLike | str,
    remote_path: os.PathLike | str,
    file_connection: FileConnection,
) -> list[PurePosixPath]:
    remote_files = []

    local_path = Path(source_path)

    if local_path.exists() and local_path.is_dir():
        for root_path, _dir_names, file_names in os.walk(local_path):
            local_root = Path(root_path)
            remote_root = remote_path / local_root.relative_to(local_path)

            for filename in file_names:
                local_filename = local_root / filename
                remote_filename = remote_root / filename
                file_connection.upload_file(local_filename, remote_filename)
                remote_files.append(remote_filename)

    if not remote_files:
        raise RuntimeError(
            f"Could not load file examples from {local_path}. Path should be exists and should contain samples",
        )

    return remote_files
