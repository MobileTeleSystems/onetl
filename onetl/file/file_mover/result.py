# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import Field

from onetl.file.file_result import FileResult, FileSet
from onetl.impl import FailedRemoteFile, RemoteFile, RemotePath


class MoveResult(FileResult):
    """
    Representation of file move result.

    Container for file paths, divided into certain categories:

    * [successful][]
    * [failed][]
    * [skipped][]
    * [missing][]

    !!! success "Added in 0.8.0"

    Examples
    --------

    ```python
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
    ```
    """

    successful: FileSet[RemoteFile] = Field(default_factory=lambda: FileSet({}))
    "File paths (local) which were moved successfully"

    failed: FileSet[FailedRemoteFile] = Field(default_factory=lambda: FileSet({}))
    "File paths (remote) which were not moved because of some failure"

    skipped: FileSet[RemoteFile] = Field(default_factory=lambda: FileSet({}))
    "File paths (remote) which were skipped because of some reason"

    missing: FileSet[RemotePath] = Field(default_factory=lambda: FileSet({}))
    "File paths (remote) which are not present in the remote file system"
