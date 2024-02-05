# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pydantic import Field

from onetl.file.file_result import FileResult, FileSet
from onetl.impl import FailedLocalFile, LocalPath, RemoteFile


class UploadResult(FileResult):
    """
    Representation of file upload result.

    Container for file paths, divided into certain categories:

    * :obj:`successful`
    * :obj:`failed`
    * :obj:`skipped`
    * :obj:`missing`

    Examples
    --------

    Upload files

    .. code:: python

        from onetl.impl import LocalPath, RemoteFile, FailedLocalFile
        from onetl.file import FileUploader, UploadResult

        uploader = FileUploader(target_path="/remote", ...)

        uploaded_files = uploader.run(
            [
                "/local/file1",
                "/local/file2",
                "/failed/file",
                "/existing/file",
                "/missing/file",
            ]
        )

        assert uploaded_files == UploadResult(
            successful={
                RemoteFile("/remote/file1"),
                RemoteFile("/remote/file2"),
            },
            failed={FailedLocalFile("/failed/file")},
            skipped={LocalPath("/existing/file")},
            missing={LocalPath("/missing/file")},
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
