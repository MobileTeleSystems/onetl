from abc import abstractmethod
from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Dict

from onetl.connection import ConnectionABC

log = getLogger(__name__)


@dataclass(frozen=True)
class DBConnection(ConnectionABC):
    driver: str = field(init=False, default='')
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = field(repr=False, default=None)
    # Database in rdbms, schema in DBReader.
    # Difference like https://www.educba.com/postgresql-database-vs-schema/
    database: str = 'default'
    extra: Dict = field(default_factory=dict)
    spark: Optional['pyspark.sql.SparkSession'] = None

    def save_df(
        self,
        df: 'pyspark.sql.DataFrame',
        table: str,
        jdbc_options: Dict,
    ):
        """
        Save the DataFrame into RDB.

        :type df: pyspark.sql.DataFrame
        """

        options = jdbc_options.copy()
        options.update(user=self.login, password=self.password, driver=self.driver)

        log_pass = 'PASSWORD="*****"' if options.get('password') else 'NO_PASSWORD'
        log.info(f'USER="{options["user"]}" {log_pass} DRIVER={options["driver"]}')
        log.info(f'JDBC_URL="{self.url}"')

        mode = options.get('mode')
        df.write.options(**options).jdbc(self.url, table, mode)

    def read_table(self, sql_text, jdbc_options):
        conf = jdbc_options.copy()

        conf['user'] = self.login
        conf['password'] = self.password
        conf['driver'] = self.driver

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

        return self.spark.read.options(**conf).jdbc(url=self.url, table=sql_text)

    def get_sql_text(self, sql_hint, columns, table, sql_where):
        statements = [
            'SELECT ',
            sql_hint,
            columns,
            f' FROM {table}',
        ]

        if sql_where:
            statements.append(f' WHERE ({sql_where})')

        return f'({" ".join(statements)}) T'

    @property
    @abstractmethod
    def url(self):
        """"""

    def get_value_sql(self, value):
        """
        Transform the value into an SQL Dialect-supported form.

        :type value: HWM
        :rtype: str
        """
        if value.value_type in {'timestamp', 'datetime'}:
            return self._get_timestamp_value_sql(value)
        return value.lit()

    @abstractmethod
    def _get_timestamp_value_sql(self, value):
        """
        Transform the value into an SQL Dialect-supported timestamp.

        :type value: Value
        :rtype: str
        """
