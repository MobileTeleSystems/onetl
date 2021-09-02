from abc import abstractmethod
from dataclasses import dataclass, field
from functools import cached_property
from typing import Optional, Any, List, Mapping

from onetl.connection import ConnectionABC


@dataclass
class FileConnection(ConnectionABC):
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    extra: Optional[Mapping] = field(default_factory=dict)
    spark: Optional['pyspark.sql.SparkSession'] = None

    @abstractmethod
    def get_client(self) -> Any:
        """"""

    @cached_property
    def client(self):
        return self.get_client()

    @abstractmethod
    def download_file(self, remote_file_path: str, local_file_path: str) -> None:
        """"""

    @abstractmethod
    def remove_file(self, remote_file_path: str):
        """"""

    @abstractmethod
    def is_dir(self, top, item):
        """"""

    @abstractmethod
    def get_name(self, item):
        """"""

    def listdir(self, path) -> List[str]:
        return [self.get_name(item) for item in self._listdir(path)]

    def walk(self, top, topdown=True, onerror=None, exclude_dirs=None):
        """
        Iterate over directory tree and return a tuple (dirpath,
        dirnames, filenames) on each iteration, like the `os.walk`
        function (see https://docs.python.org/library/os.html#os.walk ).
        """
        if not exclude_dirs:
            exclude_dirs = []
        try:
            items = self._listdir(top)
        except Exception as err:
            if onerror:
                onerror(err)
            return
        dirs, nondirs = [], []
        for item in items:
            name = self.get_name(item)
            full_name = top / name
            if self.is_dir(top, item):
                if full_name not in exclude_dirs:
                    dirs.append(name)
            else:
                nondirs.append(name)
        if topdown:
            yield top, dirs, nondirs
        for name in dirs:
            path = top / name
            yield from self.walk(path, topdown, onerror, exclude_dirs)
        if not topdown:
            yield top, dirs, nondirs

    @abstractmethod
    def _listdir(self, path):
        """"""

    # TODO: def upload_file(path)
