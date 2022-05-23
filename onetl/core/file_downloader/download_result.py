from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Set

from pydantic import Field

from onetl.core.file_result import FileResult, FileSet
from onetl.impl import FailedRemoteFile, RemoteFile


class DownloadResult(FileResult):
    """
    Representation of file download result.

    Container for file paths, divided into certain categories:

    * ``success`` - successfully handled files (local)
    * ``failed`` - file paths (remote) which were handled with some failures
    * ``skipped`` - file paths (remote) which were skipped because of some reason
    * ``missing`` - file paths (remote) which are not present in the file system

    Examples
    --------

    Download files

    .. code:: python

        from pathlib import Path, PurePath
        from onetl.impl import RemoteFile, FailedLocalFile
        from onetl.core import FileDownloader, DownloadResult

        downloader = FileDownloader(local_path="/local", ...)

        downloaded_files = downloader.run(
            [
                "/remote/file1",
                "/remote/file2",
                "/failed/file",
                "/existing/file",
                "/missing/file",
            ]
        )

        assert downloaded_files == DownloadResult(
            success={
                RemoteFile("/local/file1"),
                RemoteFile("/local/file2"),
            },
            failed={FailedLocalFile("/failed/file")},
            skipped={Path("/existing/file")},
            missing={PurePath("/missing/file")},
        )
    """

    success: FileSet[Path] = Field(default_factory=FileSet)
    failed: FileSet[FailedRemoteFile] = Field(default_factory=FileSet)
    skipped: FileSet[RemoteFile] = Field(default_factory=FileSet)
    missing: Set[PurePosixPath] = Field(default_factory=set)
