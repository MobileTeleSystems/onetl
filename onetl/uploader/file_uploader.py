import uuid
import os
from dataclasses import dataclass
from logging import getLogger
from typing import Optional, List

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


@dataclass
class FileUploader:
    connection: FileConnection
    target_path: str
    # Remote temporary path to upload files
    temp_path: Optional[str] = "/tmp/{}"  # NOSONAR

    def run(self, files_list: List[str]) -> List[str]:

        if not files_list:
            log.warning("Files list is empty. Please, provide files to upload.")
            return files_list

        if not self.connection.path_exists(self.target_path):
            self.connection.mkdir(self.target_path)

        successfully_uploaded_files = []
        current_temp_dir = self.temp_path.format(str(uuid.uuid4()))

        if not self.connection.path_exists(current_temp_dir):
            self.connection.mkdir(current_temp_dir)

        for count, file_path in enumerate(files_list):
            log.info(f"Processing {count + 1} of {len(files_list)} ")
            path, filename = os.path.split(file_path)
            tmp_file = os.path.join(current_temp_dir, filename)
            target_file = os.path.join(self.target_path, filename)
            try:
                self.connection.upload_file(file_path, tmp_file)

                self.connection.rename(tmp_file, target_file)
            except Exception as e:
                log.exception(f"Couldn't load file {file_path} to target dir {self.target_path}.\nError:\n{e}")
            else:
                successfully_uploaded_files.append(target_file)
            finally:
                self.connection.rmdir(current_temp_dir, recursive=True)

        return successfully_uploaded_files
