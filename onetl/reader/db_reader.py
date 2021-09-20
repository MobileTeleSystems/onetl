from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Union, Mapping

from onetl.connection.db_connection.db_connection import DBConnection

log = getLogger(__name__)
# TODO: сделать нормальное логирование


@dataclass
class DBReader:
    connection: DBConnection
    # table is 'schema.table'
    table: str
    columns: Optional[Union[str, list]] = None
    sql_where: Optional[str] = ''
    sql_hint: Optional[str] = ''
    jdbc_options: Optional[Mapping] = field(default_factory=dict)

    def __post_init__(self):
        self.columns = ', '.join(self.columns) if isinstance(self.columns, list) else self.columns or '*'  # noqa:WPS601

    def run(self):
        sql_text = self.get_table_sql_text()

        return self.connection.read_table(sql_text=sql_text, jdbc_options=self.jdbc_options)

    def get_table_sql_text(self):
        return self.connection.get_sql_text(
            sql_hint=self.sql_hint,
            columns=self.columns,
            sql_where=self.sql_where,
            table=self.table,
        )
