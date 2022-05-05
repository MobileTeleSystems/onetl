from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class FileStatProtocol(Protocol):
    """
    Protocol for ``os.stat_result``-like objects.

    Includes only minimal set of fields supported by any file system
    """

    @property
    def st_size(self) -> int:
        """
        Size of file, in bytes
        """

    @property
    def st_mtime(self) -> float:
        """
        Unix timestamp of most recent content modification
        """
