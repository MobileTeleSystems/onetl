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
