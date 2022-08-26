from __future__ import annotations

import operator
from datetime import date, datetime
from logging import getLogger
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict

from onetl.base import BaseDBConnection
from onetl.impl import FrozenModel
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = getLogger(__name__)


class DBConnection(BaseDBConnection, FrozenModel):

    # TODO:(@dypedchenk) Create abstract class for engine. Engine uses pyhive session or Engine uses pyspark session
    spark: SparkSession

    _check_query: ClassVar[str] = "SELECT 1"
    _compare_statements: ClassVar[Dict[Callable, str]] = {
        operator.ge: "{} >= {}",
        operator.gt: "{} > {}",
        operator.le: "{} <= {}",
        operator.lt: "{} < {}",
        operator.eq: "{} == {}",
        operator.ne: "{} != {}",
    }

    def get_sql_query(
        self,
        table: str,
        columns: list[str] | None = None,
        where: str | None = None,
        hint: str | None = None,
    ) -> str:
        """
        Generates a SQL query using input arguments
        """

        columns_str = ", ".join(columns) if columns else "*"
        hint = f"/*+ {hint} */" if hint else None
        where = f"WHERE {where}" if where else None

        return " ".join(
            filter(
                None,
                [
                    "SELECT",
                    hint,
                    columns_str,
                    "FROM",
                    table,
                    where,
                ],
            ),
        )

    def expression_with_alias(self, expression: str, alias: str) -> str:
        return f"{expression} AS {alias}"

    def get_compare_statement(self, comparator: Callable, arg1: Any, arg2: Any) -> str:
        template = self._compare_statements[comparator]
        return template.format(arg1, self._get_value_sql(arg2))

    @classmethod
    def _forward_refs(cls) -> dict[str, type]:
        # avoid importing pyspark unless user called the constructor
        refs = super()._forward_refs()
        from pyspark.sql import SparkSession  # noqa: WPS442

        refs["SparkSession"] = SparkSession

        return refs

    def _log_parameters(self):
        log.info("|Spark| Using connection parameters:")
        log_with_indent(f"type = {self.__class__.__name__}")
        parameters = self.dict(by_alias=True, exclude_none=True, exclude={"spark"})
        for attr, value in sorted(parameters.items()):
            log_with_indent(f"{attr} = {value!r}")

    def _get_value_sql(self, value: Any) -> str:
        """
        Transform the value into an SQL Dialect-supported form.
        """

        if isinstance(value, datetime):
            return self._get_datetime_value_sql(value)

        if isinstance(value, date):
            return self._get_date_value_sql(value)

        return str(value)

    def _get_datetime_value_sql(self, value: datetime) -> str:
        """
        Transform the datetime value into supported by SQL Dialect
        """
        result = value.isoformat()
        return repr(result)

    def _get_date_value_sql(self, value: date) -> str:
        """
        Transform the date value into supported by SQL Dialect
        """
        result = value.isoformat()
        return repr(result)

    def _get_max_value_sql(self, value: Any) -> str:
        """
        Generate `MAX(value)` clause for given value
        """

        result = self._get_value_sql(value)

        return f"MAX({result})"

    def _get_min_value_sql(self, value: Any) -> str:
        """
        Generate `MIN(value)` clause for given value
        """

        result = self._get_value_sql(value)

        return f"MIN({result})"
