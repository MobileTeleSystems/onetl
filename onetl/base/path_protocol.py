from typing_extensions import Protocol, runtime_checkable

from onetl.base.file_stat_protocol import FileStatProtocol


@runtime_checkable
class PathProtocol(Protocol):
    """
    Generic protocol for ``pathlib.Path``-like objects.

    Includes only minimal set of methods which allow to determine path type (file, directory) and existence
    """

    def is_dir(self) -> bool:
        """
        Checks if this path is a directory
        """

    def is_file(self) -> bool:
        """
        Checks if this path is a file
        """

    def exists(self) -> bool:
        """
        Checks if this path exists
        """


@runtime_checkable
class SizedPathProtocol(Protocol):
    """
    Protocol for ``pathlib.Path``-like file objects.

    Includes only minimal set of methods which allow to determine if file exists, or get stats, e.g. size
    """

    def is_dir(self) -> bool:
        """
        Checks if this path is a directory
        """

    def is_file(self) -> bool:
        """
        Checks if this path is a file
        """

    def exists(self) -> bool:
        """
        Checks if this path exists
        """

    def stat(self) -> FileStatProtocol:
        """
        Returns stats object with file information
        """
