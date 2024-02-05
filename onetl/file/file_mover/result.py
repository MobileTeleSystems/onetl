# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pydantic import Field

from onetl.file.file_result import FileResult, FileSet
from onetl.impl import FailedRemoteFile, RemoteFile, RemotePath


class MoveResult(FileResult):
    """
    Representation of file move result.

    Container for file paths, divided into certain categories:

    * :obj:`successful`
    * :obj:`failed`
    * :obj:`skipped`
    * :obj:`missing`

    Examples
    --------

    Move files

    .. code:: python

        from onetl.impl import RemotePath, RemoteFile, FailedLocalFile
        from onetl.file import FileMover, MoveResult

        mover = FileMover(local_path="/local", ...)

        moved_files = mover.run(
            [
                "/source/file1",
                "/source/file2",
                "/failed/file",
                "/existing/file",
                "/missing/file",
            ]
        )

        assert moved_files == MoveResult(
            successful={
                RemoteFile("/target/file1"),
                RemoteFile("/target/file2"),
            },
            failed={FailedLocalFile("/failed/file")},
            skipped={RemoteFile("/existing/file")},
            missing={RemotePath("/missing/file")},
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
