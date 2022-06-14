from __future__ import annotations

from pydantic import Field

from onetl.core import FileResult, FileSet
from onetl.impl import FailedLocalFile, LocalPath, RemoteFile


class UploadResult(FileResult):
    """
    Representation of file upload result.

    Container for file paths, divided into certain categories:

    * ``successful`` - successfully handled files (remote)
    * ``failed`` - file paths (local) which were handled with some failures
    * ``skipped`` - file paths (local) which were skipped because of some reason
    * ``missing`` - file paths (local) which are not present in the file system

    Examples
    --------

    Upload files

    .. code:: python

        from onetl.impl import LocalPath, RemoteFile, FailedLocalFile
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
    failed: FileSet[FailedLocalFile] = Field(default_factory=FileSet)
    skipped: FileSet[LocalPath] = Field(default_factory=FileSet)
    missing: FileSet[LocalPath] = Field(default_factory=FileSet)
