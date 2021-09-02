from dataclasses import dataclass, field

from onetl.connection.db_connection.db_connection import DBConnection


@dataclass
class Postgres(DBConnection):
    driver: str = field(init=False, default='org.postgresql.Driver')
    port: int = 5432

    @property
    def url(self):
        return f'jdbc:postgresql://{self.host}:{self.port}/{self.schema}'

    def _get_timestamp_value_sql(self, value):
        return f'{value.lit()}::timestamp'
