from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from logging import getLogger
import operator
from typing import Any, Callable, ClassVar, Optional, Dict, Union
from abc import abstractmethod

from pydantic import BaseModel

from onetl.connection.connection_abc import ConnectionABC


log = getLogger(__name__)


@dataclass(frozen=True)
class DBConnection(ConnectionABC):
    # TODO:(@dypedchenk) Create abstract class for engine. Engine uses pyhive session or Engine uses pyspark session
    spark: pyspark.sql.SparkSession
    check_statement: ClassVar[str] = "SELECT 1"

    compare_statements: ClassVar[Dict[Callable, str]] = {
        operator.ge: "{} >= {}",
        operator.gt: "{} > {}",
        operator.le: "{} <= {}",
        operator.lt: "{} < {}",
        operator.eq: "{} == {}",
        operator.ne: "{} != {}",
    }

    class Options(BaseModel):  # noqa: WPS431
        """Hive or JDBC options"""

        class Config:  # noqa: WPS431
            allow_population_by_field_name = True
            frozen = True
            extra = "allow"

    @property
    @abstractmethod
    def instance_url(self) -> str:
        """Instance URL"""

    @abstractmethod
    def save_df(
        self,
        df: pyspark.sql.DataFrame,
        table: str,
        mode: Optional[str] = "append",
        options: Options = None,
    ) -> None:
        """"""

    @abstractmethod
    def read_table(
        self,
        table: str,
        columns: Optional[str],
        hint: Optional[str],
        where: Optional[str],
        options: Options,
    ) -> pyspark.sql.DataFrame:
        """"""

    @abstractmethod
    def get_schema(
        self,
        table: str,
        columns: str,
        options: Options,
    ) -> pyspark.sql.types.StructType:
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

    def to_options(
        self,
        options: Union[Options, Dict],
    ) -> Options:
        """
        Сonverting <options> is performed depending on which class the <options> parameter was passed.
        If a parameter inherited from the Option class was passed, then it will be returned unchanged.
        If a Dict object was passed it will be converted to Options.
        If the JDBC connect class was used and the Hive options class was used,
        then a ValueError exception will be thrown. If it is the other way around, an exception will also be thrown.
        """
        if isinstance(options, Dict):
            options = self.Options.parse_obj(options)

        if not isinstance(options, self.Options):
            raise ValueError(
                f"{options.__class__.__name__} cannot be passed to {self.__class__.__name__}. " f"Inappropriate types.",
            )

        return options

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
