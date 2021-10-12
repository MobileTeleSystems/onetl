import os
import posixpath

from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Iterator, List

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.downloader.downloader_helper import create_local_dir, check_pattern

log = getLogger(__name__)


@dataclass
# TODO:(@mivasil6) make check_history functional
class FileDownloader:
    connection: FileConnection
    source_path: str
    local_path: str
    source_file_pattern: Optional[str] = "*"  # fnmatch check for file_name. For example: "*.csv"
    delete_source: bool = False  # parameter responsible for configuring the deletion of downloaded files on source
    source_exclude_dirs: List = field(default_factory=list)  # a list of dirs excluded from loading

    def remote_files_listing(self, source_path: str) -> Iterator:
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

                file_path = posixpath.join(root, res_file)
                log.info(f"Add file: {file_path}")
                yield file_path

    def run(self) -> List[str]:  # noqa: WPS231
        downloaded_files = []
        downloaded_remote_files = []
        files_size = 0
        last_exception = None
        create_local_dir(self.local_path)

        for remote_file_path in self.remote_files_listing(self.source_path):
            try:
                path, filename = os.path.split(remote_file_path)
                local_file_path = posixpath.join(self.local_path, filename)

                # Download
                self.connection.download_file(remote_file_path, local_file_path)

                # Delete Remote
                if self.delete_source:
                    self.connection.remove_file(remote_file_path)

                file_size = os.path.getsize(local_file_path)

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
