from abc import abstractmethod
from dataclasses import dataclass, field
from functools import wraps
import posixpath
from typing import Optional, Any, List, Mapping, Callable, Generator

from onetl.connection import ConnectionABC


# Workaround for cached_property
def cached(f):
    @wraps(f)  # NOQA: WPS430
    def wrapped(self, *args, **kwargs):  # NOQA: WPS430
        key = f"{self.__class__.__name__}_{f.__name__}_cached_val"
        existing = getattr(wrapped, key, None)
        if existing is not None:
            return existing
        result = f(self, *args, **kwargs)
        setattr(wrapped, key, result)
        return result

    return wrapped


@dataclass(frozen=True)
class FileConnection(ConnectionABC):
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    extra: Optional[Mapping] = field(default_factory=dict)
    spark: Optional["pyspark.sql.SparkSession"] = None

    @abstractmethod
    def get_client(self) -> Any:
        """"""

    @property
    @cached
    def client(self):
        return self.get_client()

    @abstractmethod
    def download_file(self, remote_file_path: str, local_file_path: str) -> None:
        """"""

    @abstractmethod
    def remove_file(self, remote_file_path: str) -> None:
        """"""

    @abstractmethod
    def is_dir(self, top, item) -> bool:
        """"""

    @abstractmethod
    def get_name(self, item) -> str:
        """"""

    @abstractmethod
    def path_exists(self, path: str) -> bool:
        """"""

    @abstractmethod
    def mk_dir(self, path: str) -> str:
        """"""

    @abstractmethod
    def upload_file(self, local_file_path: str, remote_file_path: str, *args, **kwargs) -> str:
        """"""

    def listdir(self, path: str) -> List[str]:
        return [self.get_name(item) for item in self._listdir(path)]

    def walk(
        self,
        top: str,
        topdown: bool = True,
        onerror: Callable = None,
        exclude_dirs: List[str] = None,
    ) -> Generator[str, List[str], List[str]]:
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
            full_name = posixpath.join(top, name)
            if self.is_dir(top, item):
                if not self.excluded_dir(full_name, exclude_dirs):
                    dirs.append(name)
            else:
                nondirs.append(name)
        if topdown:
            yield top, dirs, nondirs
        for name in dirs:
            path = posixpath.join(top, name)
            yield from self.walk(path, topdown, onerror, exclude_dirs)
        if not topdown:
            yield top, dirs, nondirs

    def rename(self, source: str, target: str) -> None:
        """"""

    def rmdir(self, path: str, recursive: bool) -> None:
        """"""

    def excluded_dir(self, full_name: str, exclude_dirs: List) -> bool:
        for exclude_dir in exclude_dirs:
            if exclude_dir == full_name:
                return True
        return False

    @abstractmethod
    def _listdir(self, path: str) -> List:
        """"""
