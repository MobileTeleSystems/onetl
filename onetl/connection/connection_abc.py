from abc import ABC
from dataclasses import dataclass, field
from typing import Optional, Mapping


@dataclass(frozen=True)
class ConnectionABC(ABC):
    """
    Airflow-like connection object.

    Checkout airflow.models.connection.Connection for more info.
    """
    secure: tuple = field(default=('token', 'key', 'password', 'passwd', 'pass', 'pw', 'pwd'), init=False)

    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    extra: Optional[Mapping] = field(default_factory=dict)

    def __str__(self):
        # NOTE: secure_extra cannot be placed into __init__
        #   because extra is a MutableMapping which can be changed
        #   after the Connection was created.
        secure_extra = {}
        for k, v in self.extra.items():
            if isinstance(v, str) and any(s in k.lower() for s in self.secure):
                v = 'XXX'
            secure_extra[k] = v

        return (
            f'{self.__class__.__name__}('  # noqa: WPS221
            f'host={self.host!r}, '
            f'port={self.port!r}, '
            f'login={self.login!r}, '
            "password='XXX', "
            f'database={self.database!r}, '
            f'extra={secure_extra!r}'
            ')'
        )

    def __repr__(self):
        return str(self)
