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
    mode: str = 'append'
    format: str = 'orc'
    jdbc_options: Mapping = field(default_factory=dict)

    def run(self, df):
        conf = self.jdbc_options.copy()
        conf.update(mode=self.mode, format=self.format)

        self.connection.save_df(df, self.table, conf)
