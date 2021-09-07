from abc import abstractmethod
from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Mapping

from onetl.connection import ConnectionABC

log = getLogger(__name__)


@dataclass
class DBConnection(ConnectionABC):
    driver: str = field(init=False, default='')
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    extra: Optional[Mapping] = field(default_factory=dict)
    spark: Optional['pyspark.sql.SparkSession'] = None

    def save_df(
        self,
        df: 'pyspark.sql.DataFrame',
        table: str,
        spark_write_config: Mapping,
    ):
        """
        Save the DataFrame into RDB.

        :type df: pyspark.sql.DataFrame
        """

        properties = {
            'user': self.login,
            'password': self.password,
            'driver': self.driver,
        }
        if 'properties' in spark_write_config:
            properties.update(spark_write_config['properties'])

        log_pass = 'PASSWORD="*****"' if properties.get('password') else 'NO_PASSWORD'
        log.info(f'USER="{properties["user"]}" {log_pass} DRIVER={properties["driver"]}')
        log.info(f'JDBC_URL="{self.url}"')

        mode = spark_write_config.get('mode')
        df.write.jdbc(self.url, table, mode, properties).collect()

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
