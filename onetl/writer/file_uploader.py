import uuid
from dataclasses import dataclass
from logging import getLogger
from typing import Optional

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


@dataclass
class FileUploader:
    connection: FileConnection
    target_path: str
    temp_hdfs_path: Optional[str] = "/tmp/{uuid}"  # NOSONAR

    def upload(self, files_list):

        if not files_list:
            raise ValueError("Files list is empty")

        if not self.connection.path_exists(self.target_path):
            self.connection.mk_dir(self.target_path)

        successfully_uploaded_files = []

        for count, file_path in enumerate(files_list):
            log.info(f"Processing {count + 1} of {len(files_list)} ")
            current_temp_dir = self.temp_hdfs_path.format(str(uuid.uuid4()))
            try:
                self.connection.upload_file(file_path, current_temp_dir)
            except Exception as e:
                log.exception(f"Couldn't load file {file_path} to HDFS dir {self.target_path}. Error: {e}")
            else:
                successfully_uploaded_files.append(file_path)
            finally:
                self.connection.rm(current_temp_dir, recursive=True)
