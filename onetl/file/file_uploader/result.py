# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl.file.file_result import FileResult, FileSet
from onetl.impl import FailedLocalFile, LocalPath, RemoteFile


class UploadResult(FileResult):
    """
    Representation of file upload result.

    Container for file paths, divided into certain categories:

    * :obj:`~successful`
    * :obj:`~failed`
    * :obj:`~skipped`
    * :obj:`~missing`

    .. versionadded:: 0.3.0

    Examples
    --------

    >>> from onetl.file import FileUploader
    >>> uploader = FileUploader(target_path="/remote", ...)
    >>> upload_result = uploader.run(
    ...     [
    ...         "/local/file1",
    ...         "/local/file2",
    ...         "/failed/file",
    ...         "/existing/file",
    ...         "/missing/file",
    ...     ]
    ... )
    >>> upload_result
    UploadResult(
        successful=FileSet([
            RemoteFile("/remote/file1"),
            RemoteFile("/remote/file2"),
        ]),
        failed=FileSet([
            FailedLocalFile("/failed/file")
        ]),
        skipped=FileSet([
            LocalPath("/existing/file")
        ]),
        missing=FileSet([
            LocalPath("/missing/file")
        ]),
    )
    """

    successful: FileSet[RemoteFile] = Field(default_factory=FileSet)
    "File paths (remote) which were uploaded successfully"

    failed: FileSet[FailedLocalFile] = Field(default_factory=FileSet)
    "File paths (local) which were not uploaded because of some failure"

    skipped: FileSet[LocalPath] = Field(default_factory=FileSet)
    "File paths (local) which were skipped because of some reason"

    missing: FileSet[LocalPath] = Field(default_factory=FileSet)
    "File paths (local) which are not present in the local file system"
