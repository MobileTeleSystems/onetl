from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Mapping

from onetl.connection import ConnectionABC


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
