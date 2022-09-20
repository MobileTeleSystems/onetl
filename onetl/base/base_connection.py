from abc import ABC, abstractmethod


class BaseConnection(ABC):
    """
    Generic connection class
    """

    @abstractmethod
    def check(self):
        """Check source availability.

        Raises
        ------
        RuntimeError
            If the connection is not available
        """
