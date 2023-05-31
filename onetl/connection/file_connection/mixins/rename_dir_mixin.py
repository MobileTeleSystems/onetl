#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import os
from abc import abstractmethod
from logging import getLogger

from onetl.base import BaseFileConnection
from onetl.exception import DirectoryExistsError
from onetl.impl import RemoteDirectory, RemotePath, path_repr

log = getLogger(__name__)


class RenameDirMixin(BaseFileConnection):
    def rename_dir(
        self,
        source_dir_path: os.PathLike | str,
        target_dir_path: os.PathLike | str,
        replace: bool = False,
    ) -> RemoteDirectory:
        """
        Rename or move dir on remote filesystem.

        Parameters
        ----------
        source_dir_path : str or :obj:`os.PathLike`
            Old directory path

        target_dir_path : str or :obj:`os.PathLike`
            New directory path

        replace : bool, default ``False``
            If ``True``, existing directory will be replaced.

        Returns
        -------
        New directory path with stats.

        Raises
        ------
        NotADirectoryError
            Path is not a directory

        :obj:`onetl.exception.DirectoryNotFoundError`
            Path does not exist

        :obj:`onetl.exception.DirectoryExistsError`
            Directory already exists, and ``replace=False``

        Examples
        --------

        .. code:: python

            new_file = connection.rename_dir("/path/to/dir1", "/path/to/dir2")
            assert connection.path_exists("/path/to/dir1")
            assert not connection.path_exists("/path/to/dir2")
        """

        log.debug("|%s| Renaming directory '%s' to '%s'", self.__class__.__name__, source_dir_path, target_dir_path)

        source_dir = self.resolve_dir(source_dir_path)
        target_dir = RemotePath(target_dir_path)

        if self.path_exists(target_dir):
            directory = self.resolve_dir(target_dir)
            if not replace:
                raise DirectoryExistsError(f"Directory {path_repr(directory)} already exists")

            log.warning("|%s| Directory %s already exists, removing", self.__class__.__name__, path_repr(directory))
            self.remove_dir(target_dir, recursive=True)

        self.create_dir(target_dir.parent)
        self._rename_dir(source_dir, target_dir)
        log.info("|%s| Successfully renamed file '%s' to '%s'", self.__class__.__name__, source_dir, target_dir)

        return self.resolve_dir(target_dir)

    @abstractmethod
    def _rename_dir(self, source: RemotePath, target: RemotePath) -> None:
        ...
