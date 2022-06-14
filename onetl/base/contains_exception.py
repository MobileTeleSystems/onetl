from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class ContainsException(Protocol):
    """
    Protocol for objects containing ``.exception`` attribute
    """

    def exception(self) -> Exception:
        """
        Exception object with traceback
        """