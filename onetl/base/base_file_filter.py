from __future__ import annotations

from abc import ABC, abstractmethod

from onetl.base.path_protocol import PathProtocol


class BaseFileFilter(ABC):
    """
    Base file filter class
    """

    @abstractmethod
    def match(self, path: PathProtocol) -> bool:
        """
        Checks if path should be handled
        """
