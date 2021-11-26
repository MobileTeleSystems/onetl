from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class ConnectionABC(ABC):
    """
    Airflow-like connection object.

    Checkout airflow.models.connection.Connection for more info.
    """

    @abstractmethod
    def check(self):
        """Check source availability"""
