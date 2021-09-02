from dataclasses import dataclass, field

from onetl.connection.db_connection.db_connection import DBConnection


@dataclass
class MSSQL(DBConnection):
    driver: str = field(init=False, default='com.microsoft.sqlserver.jdbc.SQLServerDriver')
    port: int = 1433

    @property
    def url(self):
        params = ''.join(f';{k}={v}' for k, v in self.extra.items())
        return f'jdbc:sqlserver://{self.host}:{self.port};databaseName={self.schema}{params}'

    def _get_timestamp_value_sql(self, value):
        return f'CAST({value.lit()} AS datetime2)'
