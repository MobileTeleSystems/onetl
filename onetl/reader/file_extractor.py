import os
import posixpath
import re

from dataclasses import dataclass
from logging import getLogger
from typing import Optional, Iterator

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.reader.reader_helper import BaseRemoteFile

log = getLogger(__name__)


@dataclass
class FileExtractor:
    connection: FileConnection
    source_path: str
    # TODO: разобрать и эту помойку
    local_stg_path: Optional[str] = None
    remote_source_file_flag_regex: Optional[str] = None
    remote_source_file_flag_download: Optional[str] = None
    delete_source: bool = False
    remote_source_max_files_in_batch: int = 1000
    remote_source_file_pattern: Optional[str] = None
    remote_source_exclude_dirs: Optional[str] = None
    remote_source_file_flag_replace: Optional[str] = None
    remote_source_check_md5: Optional[str] = None
    lookup_history: bool = True

    def remote_files_listing(self, remote_source_root_path: str) -> Iterator:
        log.info('Getting files list from remote source path: {path}'.format(path=remote_source_root_path))
        exclude_dirs = []
        if self.remote_source_exclude_dirs:
            exclude_dirs = [os.path.join(remote_source_root_path, d) for d in self.remote_source_exclude_dirs]
        try:
            ftp_walk = self.connection.walk(
                remote_source_root_path,
                topdown=True,
                onerror=log.exception,
                exclude_dirs=exclude_dirs,
            )
        except Exception as e:
            raise RuntimeError(
                f'Couldn`t read directory tree from remote dir {remote_source_root_path}. Error message: {e}',
            )

        remote_source_file_flag_regex = None
        if self.remote_source_file_flag_regex:
            remote_source_file_flag_regex = re.compile(self.remote_source_file_flag_regex)

        for root, dirs, files in ftp_walk:
            log.info(f'Listing dir f"{root}", dirs: {len(dirs)} files: {len(files)}')
            for res_file in files:
                log.info('Checking file: {}'.format(res_file))
                remote_file = BaseRemoteFile(
                    local_dir=self.local_stg_path,
                    remote_file_path=posixpath.join(root, res_file),
                    remote_source_root_path=remote_source_root_path,
                    remote_source_file_pattern=self.remote_source_file_pattern,
                    remote_source_file_flag_download=self.remote_source_file_flag_download,
                    remote_source_file_flag_regex=remote_source_file_flag_regex,
                    remote_source_file_flag_replace=self.remote_source_file_flag_replace,
                    remote_source_check_md5=self.remote_source_check_md5,
                    delete_source=self.delete_source,
                    lookup_history=self.lookup_history,
                )

                try:
                    # TODO: history, metadata
                    remote_file.check(files, self.download_files_history)
                except Exception as e:
                    log.warning(e)
                    continue

                res_files = remote_file.get_remote_names()
                res_files_str = '\n'.join(res_files)
                log.info('Add files: {}'.format(res_files_str))
                yield remote_file

    def extract(self):
        for file in self.remote_files_listing(self.source_path):
            self.connection.download_file(file, self.local_stg_path)
