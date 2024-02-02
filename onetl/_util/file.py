# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path

from onetl.exception import NotAFileError
from onetl.impl import path_repr


def get_file_hash(
    path: os.PathLike | str,
    algorithm: str,
    chunk_size: int = io.DEFAULT_BUFFER_SIZE,
) -> hashlib._Hash:
    """Get file hash by path and algorithm"""
    digest = hashlib.new(algorithm)
    with open(path, "rb") as file:
        chunk = file.read(chunk_size)
        while chunk:
            digest.update(chunk)
            chunk = file.read(chunk_size)

    return digest


def is_file_readable(path: str | os.PathLike) -> Path:
    """Check if specified path is a file and is readable"""
    path = Path(os.path.expandvars(path)).expanduser().resolve()

    if not path.exists():
        raise FileNotFoundError(f"File '{path}' does not exist")

    if not path.is_file():
        raise NotAFileError(f"{path_repr(path)} is not a file")

    if not os.access(path, os.R_OK):
        raise OSError(f"No read access to file {path_repr(path)}")

    return path
