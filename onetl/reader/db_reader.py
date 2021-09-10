from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Union, Mapping

from onetl.connection.db_connection.db_connection import DBConnection

log = getLogger(__name__)


@dataclass
class DBReader:
    connection: DBConnection
    # table is 'schema.table'
    table: str
    columns: Optional[Union[str, list]] = None
    # TODO: обсуждаемо название
    sql_where: Optional[str] = ''
    sql_hint: Optional[str] = ''
    jdbc_options: Optional[Mapping] = field(default_factory=dict)

    def __post_init__(self):
        self.columns = ', '.join(self.columns) if isinstance(self.columns, list) else self.columns or '*'  # noqa:WPS601

    def run(self):
        conf = self.jdbc_options.copy()

        conf['user'] = self.connection.login
        conf['password'] = self.connection.password
        conf['driver'] = self.connection.driver

        sql_text = self.get_table_sql_text()
        log.info(f'SQL statement: {sql_text}')

        num_partitions = conf.get('numPartitions')
        partition_column = conf.get('partitionColumn')
        lower = conf.get('lowerBound')
        upper = conf.get('upperBound')

        # TODO: возможно тоже стоит переработать
        if num_partitions is not None:
            if partition_column is None:
                log.warning("partitionColumn task parameter wasn't specified; the reading will be slowed down!")
            else:
                conf['numPartitions'] = num_partitions
                conf['column'] = partition_column
                conf['lowerBound'] = lower
                conf['upperBound'] = upper

        prepare = conf.get('sessionInitStatement')
        if prepare:
            log.info(f'Init SQL statement: {prepare}')

        return self.connection.spark.read.options(**conf).jdbc(url=self.connection.url, table=sql_text)

    def get_table_sql_text(self):
        if self.columns != '*' or self.sql_where or self.sql_hint:
            statements = [
                'SELECT ',
                self.sql_hint,
                self.columns,
                f' FROM {self.table}',
            ]

            where = []
            if self.sql_where:
                where.append(f'({self.sql_where})')
            if where:
                statements.append(' WHERE ' + ' AND '.join(where))

            sql = f'({" ".join(statements)}) T'
        else:
            sql = f'{self.table} T'
        return sql
