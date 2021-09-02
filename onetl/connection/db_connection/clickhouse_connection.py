from dataclasses import dataclass, field
from re import sub

from onetl.connection.db_connection.db_connection import DBConnection


@dataclass
class Clickhouse(DBConnection):
    driver: str = field(init=False, default='ru.yandex.clickhouse.ClickHouseDriver')
    port: int = 9000

    @property
    def url(self):
        params = '&'.join(f'{k}={v}' for k, v in self.extra.items())
        return f'jdbc:clickhouse://{self.host}:{self.port}/{self.schema}?{params}'.rstrip('?')

    def _get_timestamp_value_sql(self, value):
        value_without_fraction = sub(r"(.+)\.\d*'$", r"\1'", value.lit())
        return f"CAST('{value_without_fraction}' AS DateTime)"
