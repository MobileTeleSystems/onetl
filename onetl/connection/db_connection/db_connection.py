from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from logging import getLogger
import operator
from typing import Any, Callable, ClassVar, Optional, Dict
from abc import abstractmethod

from onetl.connection.connection_abc import ConnectionABC

log = getLogger(__name__)


@dataclass(frozen=True)
class DBConnection(ConnectionABC):
    # TODO:(@dypedchenk) Create abstract class for engine. Engine uses pyhive session or Engine uses pyspark session
    spark: "pyspark.sql.SparkSession"
    check_statement: ClassVar[str] = "select 1"

    compare_statements: ClassVar[Dict[Callable, str]] = {
        operator.ge: "{} >= {}",
        operator.gt: "{} > {}",
        operator.le: "{} <= {}",
        operator.lt: "{} < {}",
        operator.eq: "{} == {}",
        operator.ne: "{} != {}",
    }

    @abstractmethod
    def save_df(
        self,
        df: "pyspark.sql.DataFrame",
        table: str,
        mode: Optional[str] = "append",
        **kwargs: Any,
    ) -> None:
        """"""

    @abstractmethod
    def read_table(
        self,
        table: str,
        columns: Optional[str],
        hint: Optional[str],
        where: Optional[str],
        **kwargs: Any,
    ) -> "pyspark.sql.DataFrame":
        """"""

    @abstractmethod
    def get_schema(
        self,
        table: str,
        columns: str,
        **kwargs: Any,
    ) -> "pyspark.sql.types.StructType":
        """"""

    def get_compare_statement(self, comparator: Callable, arg1: Any, arg2: Any) -> str:
        template = self.compare_statements[comparator]
        return template.format(arg1, self.get_value_sql(arg2))

    def get_value_sql(self, value: Any) -> str:
        """
        Transform the value into an SQL Dialect-supported form.
        """

        if isinstance(value, datetime):
            return self._get_datetime_value_sql(value)
        elif isinstance(value, date):
            return self._get_date_value_sql(value)
        elif isinstance(value, (int, float, Decimal)):
            return str(value)

        return f"'{value}'"

    def _get_datetime_value_sql(self, value: datetime) -> str:
        """
        Transform the value into an SQL Dialect-supported datetime.
        """
        result = value.isoformat()
        return f"'{result}'"

    def _get_date_value_sql(self, value: date) -> str:
        """
        Transform the value into an SQL Dialect-supported date.
        """
        result = value.isoformat()
        return f"'{result}'"
