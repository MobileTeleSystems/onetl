# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl.file.file_result import FileResult, FileSet
from onetl.impl import FailedRemoteFile, LocalPath, RemoteFile, RemotePath


class DownloadResult(FileResult):
    """
    Representation of file download result.

    Container for file paths, divided into certain categories:

    * :obj:`~successful`
    * :obj:`~failed`
    * :obj:`~skipped`
    * :obj:`~missing`

    .. versionadded:: 0.3.0

    Examples
    --------

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
    """

    successful: FileSet[LocalPath] = Field(default_factory=FileSet)
    "File paths (local) which were downloaded successfully"

    failed: FileSet[FailedRemoteFile] = Field(default_factory=FileSet)
    "File paths (remote) which were not downloaded because of some failure"

    skipped: FileSet[RemoteFile] = Field(default_factory=FileSet)
    "File paths (remote) which were skipped because of some reason"

    missing: FileSet[RemotePath] = Field(default_factory=FileSet)
    "File paths (remote) which are not present in the remote file system"
