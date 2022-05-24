from __future__ import annotations

from pathlib import Path, PurePath

from ordered_set import OrderedSet
from pydantic import Field

from onetl.core import FileResult, FileSet
from onetl.impl import FailedLocalFile, RemoteFile


class UploadResult(FileResult):
    """
    Representation of file upload result.

    Container for file paths, divided into certain categories:

    * ``success`` - successfully handled files (remote)
    * ``failed`` - file paths (local) which were handled with some failures
    * ``skipped`` - file paths (local) which were skipped because of some reason
    * ``missing`` - file paths (local) which are not present in the file system

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
    missing: OrderedSet[PurePath] = Field(default_factory=OrderedSet)
