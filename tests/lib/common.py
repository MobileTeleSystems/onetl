import hashlib
import logging
import os
import posixpath

log = logging.getLogger(__name__)


def hashfile(file):
    buf_size = 65536
    sha256 = hashlib.sha256()

    with open(file, "rb") as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break

            sha256.update(data)
    return sha256.hexdigest()


def upload_files(source_path, remote_path, connection):
    has_files = False
    remote_files = set()

    if os.path.isdir(source_path):
        connection.mkdir(remote_path)
        for dir_path, dir_names, file_names in os.walk(source_path):
            rel_local = os.path.relpath(dir_path, source_path).replace("\\", "/")
            remote_dir = posixpath.abspath(posixpath.join(remote_path, rel_local))

            for sub_dir in dir_names:
                connection.mkdir(posixpath.join(remote_dir, sub_dir))

            for filename in file_names:
                has_files = True
                local_filename = os.path.join(dir_path, filename)
                remote_filename = posixpath.join(remote_dir, filename)
                log.info(f"Copying {local_filename} to {remote_filename}")
                connection.upload_file(local_filename, remote_filename)
                remote_files.add(remote_filename)

        if not has_files:
            raise RuntimeError(
                f"Could not load file examples from {source_path}. Path should be exists and should contain samples",
            )
