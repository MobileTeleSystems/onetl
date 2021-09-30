from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Union, List, Dict

from onetl.connection.db_connection.db_connection import DBConnection

log = getLogger(__name__)
# TODO: implement logging


@dataclass
class DBReader:
    connection: DBConnection
    # table is 'schema.table'
    table: str
    columns: Optional[Union[str, List]] = '*'
    # TODO: name is not final
    sql_where: Optional[str] = ''
    sql_hint: Optional[str] = ''
    jdbc_options: Dict = field(default_factory=dict)

    def run(self) -> 'pyspark.sql.DataFrameReader':
        """
        :rtype pyspark.sql.DataFrameReader
        """

        return self.connection.read_table(
            jdbc_options=self.jdbc_options,
            sql_hint=self.sql_hint,
            columns=', '.join(self.columns) if isinstance(self.columns, list) else self.columns,
            sql_where=self.sql_where,
            table=self.table,
        )
