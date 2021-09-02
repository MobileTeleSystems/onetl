from dataclasses import dataclass, field
from re import sub

from onetl.connection.db_connection.db_connection import DBConnection


@dataclass
class Oracle(DBConnection):
    driver: str = field(init=False, default='oracle.jdbc.driver.OracleDriver')
    port: int = 1521

    @property
    def url(self):
        if 'sid' in self.extra:
            url = f'jdbc:oracle:thin:@{self.host}:{self.port}:{self.extra["sid"]}'
        elif 'service_name' in self.extra:
            url = f'jdbc:oracle:thin:@//{self.host}:{self.port}/{self.extra["service_name"]}'
        else:
            raise RuntimeError('Connection to Oracle does not have sid or service_name')
        return url

    def _get_timestamp_value_sql(self, value):
        value_without_fraction = sub(r"(.+)\.\d*'$", r"\1'", value.lit())  # TODO: add timestamp with fraction
        return f"TO_DATE({value_without_fraction}, 'YYYY-MM-DD HH24:MI:SS')"
