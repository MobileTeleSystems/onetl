from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class BaseConnection(ABC):
    """
    Generic connection class
    """

    @abstractmethod
    def check(self) -> None:
        """Check source availability.

        Raises
        ------
        RuntimeError
            If the connection is not available
        """
