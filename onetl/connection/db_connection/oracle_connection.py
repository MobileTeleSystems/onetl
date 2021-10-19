from dataclasses import dataclass, field
from re import sub
from typing import Optional

from onetl.connection.db_connection.db_connection import DBConnection


@dataclass(frozen=True)
class Oracle(DBConnection):
    driver: str = field(init=False, default="oracle.jdbc.driver.OracleDriver")
    port: int = 1521
    sid: Optional[str] = None
    service_name: Optional[str] = None

    @property
    def url(self) -> str:
        if self.sid:
            url = f"jdbc:oracle:thin:@{self.host}:{self.port}:{self.sid}"
        elif self.service_name:
            url = f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service_name}"
        else:
            raise ValueError("Connection to Oracle does not have sid or service_name")
        return url

    def _get_timestamp_value_sql(self, value):
        value_without_fraction = sub(r"(.+)\.\d*'$", r"\1'", value.lit())  # NOSONAR
        return f"TO_DATE({value_without_fraction}, 'YYYY-MM-DD HH24:MI:SS')"
