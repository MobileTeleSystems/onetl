from typing import Generic, TypeVar

from onetl.base import SizedPathProtocol

T = TypeVar("T", bound=SizedPathProtocol)


class FileSet(set, Generic[T]):  # noqa: WPS600
    """
    Set of pathlib-like objects.

    Just like a generic set, but with additional ``total_size`` helper method
    """

    @property
    def total_size(self) -> int:
        """
        Get total size (in bytes) of files in the set

        Examples
        --------

        .. code:: python

            from pathlib import Path
            from onet.core.file_result.file_set import FileSet

            file_set = FileSet({Path("/some/file"), Path("/some/another.file")})

            assert file_set.total_size == 1_000_000  # in bytes
        """

        return sum(file.stat().st_size for file in self if file.exists())
