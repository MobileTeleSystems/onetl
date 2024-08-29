# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import io
import os
from datetime import datetime
from pathlib import Path, PurePath

from onetl.exception import NotAFileError
from onetl.impl import path_repr

# e.g. 20230524122150
DATETIME_FORMAT = "%Y%m%d%H%M%S"


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


def generate_temp_path(root: PurePath) -> PurePath:
    """
    Returns prefix which will be used for creating temp directory

    Returns
    -------
    RemotePath
        Temp path, containing current host name, process name and datetime

    Examples
    --------

    >>> from etl_entities.process import Process
    >>> from pathlib import Path
    >>> generate_temp_path(Path("/tmp")) # doctest: +SKIP
    Path("/tmp/onetl/currenthost/myprocess/20230524122150")
    >>> with Process(dag="mydag", task="mytask"): # doctest: +SKIP
    ...    generate_temp_path(Path("/abc"))
    Path("/abc/onetl/currenthost/mydag.mytask.myprocess/20230524122150")
    """

    from etl_entities.process import ProcessStackManager

    current_process = ProcessStackManager.get_current()
    current_dt = datetime.now().strftime(DATETIME_FORMAT)
    return root / "onetl" / current_process.host / current_process.full_name / current_dt
