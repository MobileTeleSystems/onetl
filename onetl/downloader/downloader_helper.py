import os
from pathlib import PosixPath
from logging import getLogger

log = getLogger(__name__)


def create_local_dir(local_path: str) -> None:
    try:
        os.makedirs(local_path, exist_ok=True)
        log.info(f"|Local FS| Created directory: {local_path}")
    except Exception as last_exception:
        log.error(f"|Local FS| Cannot create directory: {local_path}. Exception:\n{last_exception}")
        raise last_exception


def check_pattern(res_file: str, remote_source_file_pattern: str = None) -> bool:
    if not remote_source_file_pattern or PosixPath(res_file).match(remote_source_file_pattern):
        return True
    raise RuntimeError(f"File is not matched with pattern: {remote_source_file_pattern}")
