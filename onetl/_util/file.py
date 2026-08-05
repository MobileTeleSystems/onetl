# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import hashlib
import io
import os
from datetime import datetime
from typing import TypeVar

from onetl._util.process import get_process_info
from onetl.base.path_protocol import PathProtocol
from onetl.base.pure_path_protocol import PurePathProtocol
from onetl.exception import NotAFileError
from onetl.impl import LocalPath, path_repr

# e.g. 20230524122150
DATETIME_FORMAT = "%Y%m%d%H%M%S"


def get_file_hash(
    path: LocalPath,
    algorithm: str,
    chunk_size: int = io.DEFAULT_BUFFER_SIZE,
) -> "hashlib._Hash":
    """Get file hash by path and algorithm"""
    digest = hashlib.new(algorithm)
    with path.open("rb") as file:
        chunk = file.read(chunk_size)
        while chunk:
            digest.update(chunk)
            chunk = file.read(chunk_size)

    return digest


def readable_local_file(path: LocalPath) -> LocalPath:
    """Check if specified path is a file and is readable"""
    if not path.exists():
        msg = f"File '{path}' does not exist"
        raise FileNotFoundError(msg)

    if not path.is_file():
        msg = f"{path_repr(path)} is not a file"
        raise NotAFileError(msg)

    if not os.access(path, os.R_OK):
        msg = f"No read access to file {path_repr(path)}"
        raise OSError(msg)

    return path


T = TypeVar("T", PurePathProtocol, PathProtocol)


def generate_temp_path(root: T) -> T:
    """
    Returns prefix which will be used for creating temp directory

    Returns
    -------
    RemotePath
        Temp path, containing current host name, process name and datetime

    Examples
    --------

    ```python
    >>> from pathlib import Path
    >>> generate_temp_path(Path("/tmp")) # doctest: +SKIP
    Path("/tmp/onetl/currenthost/myprocess/20230524122150")

    ```
    """

    process_name, hostname = get_process_info()
    current_dt = datetime.now().strftime(DATETIME_FORMAT)  # noqa: DTZ005
    return root / "onetl" / hostname / process_name / current_dt


def absolute_path(path: T) -> T:
    if "~" in path.parts:
        msg = "Path cannot contain `~`"
        raise ValueError(msg)

    if ".." in path.parts:
        msg = "Path cannot contain `..`"
        raise ValueError(msg)

    if not path.is_absolute():
        msg = "Path should be absolute"
        raise ValueError(msg)

    return path
