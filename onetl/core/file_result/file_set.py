from typing import Generic, TypeVar

from ordered_set import OrderedSet

from onetl.base import SizedPathProtocol

T = TypeVar("T", bound=SizedPathProtocol)


class FileSet(OrderedSet, Generic[T]):  # noqa: WPS600
    """
    Ordered set of pathlib-like objects.

    It has all the methods of generic set (e.g. ``add``, ``difference``, ``intersection``),
    as well as list (e.g. ``append``, ``index``, ``[]``).

    It also has a ``total_size`` helper method.
    """

    @property
    def total_size(self) -> int:
        """
        Get total size (in bytes) of files in the set

        Examples
        --------

        .. code:: python

            from pathlib import Path
            from onet.core import FileSet

            file_set = FileSet({Path("/some/file"), Path("/some/another.file")})

            assert file_set.total_size == 1_000_000  # in bytes
        """

        return sum(file.stat().st_size for file in self if file.exists())
