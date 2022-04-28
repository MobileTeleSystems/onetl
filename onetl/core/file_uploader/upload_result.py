from __future__ import annotations

from pathlib import Path, PurePath
from typing import Set

from pydantic import Field

from onetl.core.file_result import FileResult, FileSet
from onetl.impl import FailedLocalFile, RemoteFile


class UploadResult(FileResult):
    """
    Representation of file upload result.

    Container for file paths, divided into certain categories:

    * ``success`` - successfully handled files
    * ``failed`` - file paths which were handled with some failures
    * ``skipped`` - file paths which were skipped because of some reason
    * ``missing`` - unknown paths which cannot be handled

    Examples
    --------

    Upload files

    .. code:: python

        from pathlib import Path, PurePath
        from onetl.impl import RemoteFile, FailedLocalFile
        from onetl.core import FileUploader, UploadResult

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
            success={
                RemoteFile("/remote/file1"),
                RemoteFile("/remote/file2"),
            },
            failed={FailedLocalFile("/failed/file")},
            skipped={Path("/existing/file")},
            missing={PurePath("/missing/file")},
        )
    """

    success: FileSet[RemoteFile] = Field(default_factory=FileSet)
    failed: FileSet[FailedLocalFile] = Field(default_factory=FileSet)
    skipped: FileSet[Path] = Field(default_factory=FileSet)
    missing: Set[PurePath] = Field(default_factory=set)
