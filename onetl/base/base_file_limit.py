from __future__ import annotations

from abc import ABC, abstractmethod


class BaseFileLimit(ABC):
    """
    Base file limit class
    """

    @abstractmethod
    def reset_state(self):
        """
        resets the value
        """

    @abstractmethod
    def verify(self) -> bool:
        """
        checks the status and increment counter
        """

    @abstractmethod
    def is_reached(self) -> bool:
        """
        Show the status
        """

    @abstractmethod
    def _increase_counter(self):
        """
        increases the value
        """
