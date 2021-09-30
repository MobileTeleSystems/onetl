from dataclasses import dataclass, field
from logging import getLogger
from typing import Mapping

from onetl.connection.db_connection.db_connection import DBConnection

log = getLogger(__name__)
# TODO: сделать нормальное логирование


@dataclass
class DBWriter:
    connection: DBConnection
    table: str
    format: str = 'orc'
    mode: str = 'append'
    jdbc_options: Mapping = field(default_factory=dict)

    def run(self, df):
        jdbc_options = self.jdbc_options.copy()
        jdbc_options.update(mode=self.mode, format=self.format)

        self.connection.save_df(
            df=df,
            table=self.table,
            jdbc_options=jdbc_options,
        )
