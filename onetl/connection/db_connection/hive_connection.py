from dataclasses import dataclass
from typing import Mapping, Optional

from onetl.connection.db_connection.db_connection import DBConnection


@dataclass(frozen=True)
class Hive(DBConnection):
    port: int = 10000

    @property
    def url(self):
        params = '&'.join(f'{k}={v}' for k, v in self.extra.items())

        return f'hiveserver2://{self.login}:{self.password}@{self.host}:{self.port}?{params}'

    def save_df(
        self,
        df: 'pyspark.sql.DataFrame',
        table: str,
        jdbc_options: Optional[Mapping],
    ):
        df.write.saveAsTable(table)

    def _get_timestamp_value_sql(self, value):
        return value
