# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl.file.file_result import FileResult, FileSet
from onetl.impl import FailedRemoteFile, RemoteFile, RemotePath


class MoveResult(FileResult):
    """
    Representation of file move result.

    Container for file paths, divided into certain categories:

    * :obj:`~successful`
    * :obj:`~failed`
    * :obj:`~skipped`
    * :obj:`~missing`

    .. versionadded:: 0.8.0

    Examples
    --------

    >>> from onetl.file import FileMover
    >>> mover = FileMover(local_path="/local", ...)
    >>> move_result = mover.run(
    ...     [
    ...         "/source/file1",
    ...         "/source/file2",
    ...         "/failed/file",
    ...         "/existing/file",
    ...         "/missing/file",
    ...     ]
    ... )
    >>> move_result
    MoveResult(
        successful=FileSet([
            RemoteFile("/target/file1"),
            RemoteFile("/target/file2"),
        ]),
        failed=FileSet([
            FailedLocalFile("/failed/file")
        ]),
        skipped=FileSet([
            RemoteFile("/existing/file")
        ]),
        missing=FileSet([
            RemotePath("/missing/file")
        ]),
    )
    """

    successful: FileSet[RemoteFile] = Field(default_factory=FileSet)
    "File paths (local) which were moved successfully"

    failed: FileSet[FailedRemoteFile] = Field(default_factory=FileSet)
    "File paths (remote) which were not moved because of some failure"

    skipped: FileSet[RemoteFile] = Field(default_factory=FileSet)
    "File paths (remote) which were skipped because of some reason"

    missing: FileSet[RemotePath] = Field(default_factory=FileSet)
    "File paths (remote) which are not present in the remote file system"
