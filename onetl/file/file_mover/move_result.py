#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
