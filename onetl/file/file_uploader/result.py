# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import Field

from onetl.file.file_result import FileResult, FileSet
from onetl.impl import FailedLocalFile, LocalPath, RemoteFile


class UploadResult(FileResult):
    """
    Representation of file upload result.

    Container for file paths, divided into certain categories:

    * [successful][]
    * [failed][]
    * [skipped][]
    * [missing][]

    !!! success "Added in 0.3.0"

    Examples
    --------

    ```python
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
    ```
    """

    successful: FileSet[RemoteFile] = Field(default_factory=lambda: FileSet({}))
    "File paths (remote) which were uploaded successfully"

    failed: FileSet[FailedLocalFile] = Field(default_factory=lambda: FileSet({}))
    "File paths (local) which were not uploaded because of some failure"

    skipped: FileSet[LocalPath] = Field(default_factory=lambda: FileSet({}))
    "File paths (local) which were skipped because of some reason"

    missing: FileSet[LocalPath] = Field(default_factory=lambda: FileSet({}))
    "File paths (local) which are not present in the local file system"
