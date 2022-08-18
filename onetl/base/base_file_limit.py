from __future__ import annotations

from abc import ABC, abstractmethod

from onetl.base.path_protocol import PathProtocol


class BaseFileLimit(ABC):
    """
    Base file limit class
    """

    @abstractmethod
    def reset(self):
        """
        Resets the internal state
        """

    @abstractmethod
    def stops_at(self, path: PathProtocol) -> bool:
        """
        Update internal state and check if it is reached. Returns ``True`` if limit is reached
        """

    @property
    @abstractmethod
    def is_reached(self) -> bool:
        """
        Returns ``True`` if limit is reached
        """
