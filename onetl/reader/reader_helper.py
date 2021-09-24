import fnmatch
import os
from typing import List
from logging import getLogger

log = getLogger(__name__)


# TODO: возможно выпилить это, как минимум разобрать
class BaseRemoteFile:
    remote_flag_file_path = None

    def __init__(
        self,
        remote_file_path,
        local_dir,
        remote_source_root_path,
        remote_source_file_pattern,
        remote_source_file_flag_download,
        remote_source_file_flag_regex,
        remote_source_file_flag_replace,
        remote_source_check_md5,
        delete_source,
        lookup_history,
    ):
        self.remote_file_path = remote_file_path
        self.local_dir = local_dir
        self.remote_source_root_path = remote_source_root_path
        self.remote_source_file_pattern = remote_source_file_pattern
        self.remote_source_file_flag_download = remote_source_file_flag_download
        self.remote_source_file_flag_regex = remote_source_file_flag_regex
        self.remote_source_file_flag_replace = remote_source_file_flag_replace
        self.remote_source_check_md5 = remote_source_check_md5
        self.delete_source = delete_source
        self.lookup_history = lookup_history

    @property
    def local_file_path(self):
        return self._get_local(self.remote_file_path)

    @property
    def local_file_path_tmp(self):
        return self.local_file_path + '.downloading'

    @property
    def local_flag_file_path(self):
        result = None
        if self.remote_flag_file_path:
            result = self._get_local(self.remote_flag_file_path)

        return result

    @property
    def local_flag_file_path_tmp(self):
        result = None
        if self.remote_flag_file_path:
            result = self.local_flag_file_path + '.downloading'

        return result

    @property
    def all_local_paths(self):
        return [
            self.local_file_path,
            self.local_file_path_tmp,
            self.local_flag_file_path,
            self.local_flag_file_path_tmp,
        ]

    # TODO: Это просто ад, тут нужно все переписать
    def check(self, files: List[str], check_history: bool = True):
        """
        Проверяет соответвия файла на условиям загрузки
        :rtype: bool
        """
        self.check_flag(self.remote_file_path, files)
        self.check_pattern(self.remote_file_path)
        if check_history:
            self.check_history()

        if self.remote_source_file_flag_download:
            self.check_pattern(self.remote_flag_file_path)
            if check_history:
                self.check_history()

        return True

    def check_flag(self, flag_file, files_list):
        if not self.remote_source_file_flag_regex:
            return

        data_file = self.remote_source_file_flag_regex.sub(self.remote_source_file_flag_replace, flag_file)

        if data_file == flag_file:
            raise RuntimeError('File {flag_file} is not match with flag pattern'.format(**locals()))

        if data_file not in files_list:
            raise RuntimeError(
                'Found flag {flag_file} but file {data_file} does not exist'.format(**locals()),
            )

        self.remote_file_path = data_file
        # todo: максимально не очевидное переопределние, лучше переписать
        self.remote_flag_file_path = flag_file  # noqa: WPS601

    # TODO: понять где хранить метаинформацию
    def check_history(self):
        if not self.lookup_history:
            return True

        raise RuntimeError('Already downloaded')

    def check_pattern(self, res_file):
        if not self.remote_source_file_pattern:
            return True
        else:
            for file_pattern in self.remote_source_file_pattern:
                if fnmatch.fnmatch(res_file, file_pattern):
                    return True
        raise RuntimeError('File is not matched with patterns')

    def get_remote_names(self):
        res = {self.remote_file_path}
        if self.remote_source_file_flag_download:
            res.add(self.remote_flag_file_path)
        return res

    def check_hash(self):
        data_file = self.local_file_path_tmp
        hash_file = self.local_flag_file_path_tmp

        # md5_expected
        with open(hash_file, 'rb') as fp:
            md5_expected = str(fp.read(), 'utf-8').strip().lower()

        # md5_actual
        md5_hash = hashlib.md5()  # NOQA S303
        with open(data_file, 'rb') as fd:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: fd.read(4096), b''):  # noqa: WPS426, WPS432
                md5_hash.update(byte_block)
            md5_actual = md5_hash.hexdigest().lower()

        log.info(f'MD5 actual={md5_actual} expected={md5_expected}')

        # cleanup
        if not self.remote_source_file_flag_download:
            os.remove(hash_file)

        if md5_actual != md5_expected:
            raise RuntimeError('Skip. Invalid md5 hash')

    def create_local_dir(self):
        dir_name = os.path.dirname(self.local_file_path)
        try:
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
                log.info(f'Created directory {dir_name}')
        except Exception as last_exception:
            log.error(f'Cannot create directory {dir_name}. Exception: {last_exception}')
            raise last_exception

    def _get_local(self, remote):
        path, filename = os.path.split(remote)
        if os.path.isabs(path):
            path = os.path.relpath(path, self.remote_source_root_path)
        loc = os.path.join(self.local_dir, path, filename)
        return os.path.realpath(loc)
