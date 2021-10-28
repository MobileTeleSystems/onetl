from __future__ import annotations

from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path, PosixPath
from typing import Optional, Iterator, List

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.downloader.downloader_helper import create_local_dir, check_pattern

log = getLogger(__name__)


@dataclass
# TODO:(@mivasil6) make check_history functional
class FileDownloader:
    """Class specifies file source from where you can download files. Download files **only** in local directory.

    Parameters
    ----------
    connection : onetl.connection.file_connection.FileConnection
        Class which contain File system connection properties. See in FileConnection section.
    source_path : str
        Path on remote source where you get files.
    local_path : str
        Local path where you download files
    source_file_pattern : str, optional, default: ``*``
        Fnmatch check for file_name. For example: ``*.csv``.
    delete_source : bool, optional, default: ``False``
        Parameter responsible for configuring the deletion of downloaded files on source.
    source_exclude_dirs : list of str, optional, default: ``None``
        A list of dirs excluded from loading. Must contain full path to excluded dir.

    Examples
    --------
    Simple Downloader creation

    .. code::

        from onetl.downloader import FileDownloader
        from onetl.connection.file_connection import SFTP

        sftp = SFTP(...)

        downloader = FileDownloader(
            connection=sftp,
            source_path="/path/to/remote/source",
            local_path="/path/to/local",
        )

    Downloader with all parameters

    .. code::

        from onetl.downloader import FileDownloader
        from onetl.connection.file_connection import SFTP

        sftp = SFTP(...)

        downloader = FileDownloader(
            connection=sftp,
            source_path="/path/to/remote/source",
            local_path="/path/to/local",
            delete_source=True,
            source_exclude_dirs=["path/to/remote/source/exclude_dir"],
            source_file_pattern="*.txt",
        )
    """

    connection: FileConnection
    source_path: Path | str
    local_path: Path | str
    source_file_pattern: Optional[str] = "*"
    delete_source: bool = False
    source_exclude_dirs: List = field(default_factory=list)

    def __post_init__(self):
        self.source_path = PosixPath(self.source_path)
        self.local_path = Path(self.local_path)

    def remote_files_listing(self, source_path: Path | str) -> Iterator:
        log.info(f"Getting files list from remote source path: {source_path}")

        try:
            ftp_walk = self.connection.walk(
                source_path,
                topdown=True,
                onerror=log.exception,
                exclude_dirs=self.source_exclude_dirs,
            )
        except Exception as e:
            raise RuntimeError(
                f"Couldn`t read directory tree from remote dir {source_path}.\n Error message:\n {e}",
            )

        for root, dirs, files in ftp_walk:
            log.debug(f'Listing dir f"{root}", dirs: {len(dirs)} files: {len(files)}')
            for res_file in files:
                log.info(f"Checking file: {res_file}")
                try:
                    check_pattern(res_file, self.source_file_pattern)
                except Exception as e:
                    log.warning(e)
                    continue

                file_path = PosixPath(root) / res_file
                log.info(f"Add file: {file_path}")
                yield file_path

    def run(self) -> List[Path]:  # noqa: WPS231
        """
        Method for downloading files from source to local directory.

        Returns
        -------
        downloaded_files : List[str]
            List of downloaded files

        Examples
        --------

        Download files

        .. code::

            downloaded_files = downloader.run()

        """
        downloaded_files = []
        downloaded_remote_files = []
        files_size = 0
        last_exception = None
        create_local_dir(self.local_path)

        for remote_file_path in self.remote_files_listing(self.source_path):
            try:
                filename = remote_file_path.name
                local_file_path = PosixPath(self.local_path) / filename

                # Download
                self.connection.download_file(remote_file_path, local_file_path)

                # Delete Remote
                if self.delete_source:
                    self.connection.remove_file(remote_file_path)

                file_size = local_file_path.stat().st_size

            except Exception as e:
                last_exception = e
                log.error(
                    f"Download file {remote_file_path} from remote to {self.local_path} failed with:\n{last_exception}",
                )
            else:
                downloaded_files.append(local_file_path)
                downloaded_remote_files.append(remote_file_path)
                files_size += file_size

        log.info(f"Batch: {len(downloaded_files)} file(s) {files_size / 1024 / 1024:.3f}Mb")

        if not downloaded_files and not last_exception:
            log.warning("There are no files on remote server")
        if last_exception:
            log.error("There are some errors with files. Check previous logs.")
            raise last_exception

        return downloaded_files
