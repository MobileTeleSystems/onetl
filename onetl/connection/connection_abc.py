from abc import ABC
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ConnectionABC(ABC):
    """
    Airflow-like connection object.

    Checkout airflow.models.connection.Connection for more info.
    """

    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
