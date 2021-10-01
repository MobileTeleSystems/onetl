from dataclasses import dataclass, field
from onetl.connection.db_connection.db_connection import DBConnection


@dataclass(frozen=True)
class Postgres(DBConnection):
    driver: str = field(init=False, default="org.postgresql.Driver")
    port: int = 5432

    @property
    def url(self):
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def _get_timestamp_value_sql(self, value):
        return f"{value.lit()}::timestamp"
