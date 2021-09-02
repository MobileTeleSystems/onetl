from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Union, Mapping

from onetl.connection.db_connection.db_connection import DBConnection

log = getLogger(__name__)


@dataclass
class DBReader:
    connection: DBConnection
    table: str
    columns: Optional[Union[str, list]] = None
    # TODO: обсуждаемо название
    sql_where: Optional[str] = ''
    sql_hint: Optional[str] = ''
    check_source: bool = False
    crutch_oracle: bool = False
    spark_read_conf: Optional[Mapping] = field(default_factory=dict)

    def __post_init__(self):
        self.columns = ', '.join(self.columns) if isinstance(self.columns, list) else self.columns or '*'

    def run(self):
        conf = self.spark_read_conf.copy()

        conf['properties']['user'] = self.connection.login
        conf['properties']['password'] = self.connection.password
        conf['properties']['driver'] = self.connection.driver

        sql_text = self.get_sql_text()
        log.info(f'SQL statement: {sql_text}')

        num_partitions = conf.get('numPartitions')
        partition_column = conf.get('column')
        lower = conf.get('lowerBound')
        upper = conf.get('upperBound')

        # TODO: возможно тоже стоит переработать
        if num_partitions is not None:
            if partition_column is None:
                log.warning("read_column task parameter wasn't specified; the reading will be slowed down!")
            else:
                conf['numPartitions'] = num_partitions
                conf['column'] = partition_column
                conf['lowerBound'] = lower
                conf['upperBound'] = upper

        prepare = conf['properties'].get('sessionInitStatement')
        if prepare:
            log.info(f'Init SQL statement: {prepare}')

        return self.connection.spark.read.jdbc(url=self.connection.url, table=sql_text, **conf)

    def get_sql_text(self):
        if self.columns != '*' or self.sql_where or self.sql_hint:
            statements = [
                'SELECT ',
                self.sql_hint,
                self.columns,
                ' FROM {0}'.format(self.table),
            ]

            where = []
            if self.sql_where:
                where.append('({0})'.format(self.sql_where))
            if where:
                statements.append(' WHERE ' + ' AND '.join(where))

            sql = '({sql}) T'.format(sql=' '.join(statements))
        else:
            sql = '{sql} T'.format(sql=self.table)
        return sql
