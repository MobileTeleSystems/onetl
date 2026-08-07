# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import Field

from onetl.file.file_result import FileResult, FileSet
from onetl.impl import FailedRemoteFile, LocalPath, RemoteFile, RemotePath


class DownloadResult(FileResult):
    """
    Representation of file download result.

    Container for file paths, divided into certain categories:

    * [successful][]
    * [failed][]
    * [skipped][]
    * [missing][]

    !!! success "Added in 0.3.0"

    Examples
    --------

    ```python
    >>> from onetl.file import FileDownloader
    >>> downloader = FileDownloader(local_path="/local", ...)
    >>> download_result = downloader.run(
    ...     [
    ...         "/remote/file1",
    ...         "/remote/file2",
    ...         "/failed/file",
    ...         "/existing/file",
    ...         "/missing/file",
    ...     ]
    ... )
    >>> download_result
    DownloadResult(
        successful=FileSet([
            LocalPath("/local/file1"),
            LocalPath("/local/file2"),
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

    successful: FileSet[LocalPath] = Field(default_factory=lambda: FileSet({}))
    "File paths (local) which were downloaded successfully"

    failed: FileSet[FailedRemoteFile] = Field(default_factory=lambda: FileSet({}))
    "File paths (remote) which were not downloaded because of some failure"

    skipped: FileSet[RemoteFile] = Field(default_factory=lambda: FileSet({}))
    "File paths (remote) which were skipped because of some reason"

    missing: FileSet[RemotePath] = Field(default_factory=lambda: FileSet({}))
    "File paths (remote) which are not present in the remote file system"
