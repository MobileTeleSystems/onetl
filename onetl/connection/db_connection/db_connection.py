#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import operator
import textwrap
from datetime import date, datetime
from logging import getLogger
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict

from pydantic import Field

from onetl.base import BaseDBConnection
from onetl.hwm import Statement
from onetl.impl import FrozenModel
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = getLogger(__name__)


class DBConnection(BaseDBConnection, FrozenModel):
    spark: SparkSession = Field(repr=False)

    _check_query: ClassVar[str] = "SELECT 1"

    class Dialect(BaseDBConnection.Dialect):
        @classmethod
        def _expression_with_alias(cls, expression: str, alias: str) -> str:
            return f"{expression} AS {alias}"

        @classmethod
        def _get_compare_statement(cls, comparator: Callable, arg1: Any, arg2: Any) -> Any:
            template = cls._compare_statements[comparator]
            return template.format(arg1, cls._serialize_datetime_value(arg2))

        @classmethod
        def _merge_conditions(cls, conditions: list[Any]) -> Any:
            if len(conditions) == 1:
                return conditions[0]

            return " AND ".join(f"({item})" for item in conditions)

        @classmethod
        def _condition_assembler(
            cls,
            condition: Any,
            start_from: Statement | None,
            end_at: Statement | None,
        ) -> Any:
            conditions = [condition]

            if start_from:
                condition1 = cls._get_compare_statement(
                    comparator=start_from.operator,
                    arg1=start_from.expression,
                    arg2=start_from.value,
                )
                conditions.append(condition1)

            if end_at:
                condition2 = cls._get_compare_statement(
                    comparator=end_at.operator,
                    arg1=end_at.expression,
                    arg2=end_at.value,
                )
                conditions.append(condition2)

            result: list[Any] = list(filter(None, conditions))
            if not result:
                return None

            return cls._merge_conditions(result)

        _compare_statements: ClassVar[Dict[Callable, str]] = {
            operator.ge: "{} >= {}",
            operator.gt: "{} > {}",
            operator.le: "{} <= {}",
            operator.lt: "{} < {}",
            operator.eq: "{} == {}",
            operator.ne: "{} != {}",
        }

        @classmethod
        def _serialize_datetime_value(cls, value: Any) -> str | int | dict:
            """
            Transform the value into an SQL Dialect-supported form.
            """

            if isinstance(value, datetime):
                return cls._get_datetime_value_sql(value)

            if isinstance(value, date):
                return cls._get_date_value_sql(value)

            return str(value)

        @classmethod
        def _get_datetime_value_sql(cls, value: datetime) -> str:
            """
            Transform the datetime value into supported by SQL Dialect
            """
            result = value.isoformat()
            return repr(result)

        @classmethod
        def _get_date_value_sql(cls, value: date) -> str:
            """
            Transform the date value into supported by SQL Dialect
            """
            result = value.isoformat()
            return repr(result)

        @classmethod
        def _get_max_value_sql(cls, value: Any) -> str:
            """
            Generate `MAX(value)` clause for given value
            """
            result = cls._serialize_datetime_value(value)
            return f"MAX({result})"

        @classmethod
        def _get_min_value_sql(cls, value: Any) -> str:
            """
            Generate `MIN(value)` clause for given value
            """
            result = cls._serialize_datetime_value(value)
            return f"MIN({result})"

    @classmethod
    def _forward_refs(cls) -> dict[str, type]:
        # avoid importing pyspark unless user called the constructor,
        # as we allow user to use `Connection.package` for creating Spark session

        refs = super()._forward_refs()
        try:
            from pyspark.sql import SparkSession  # noqa: WPS442
        except (ImportError, NameError) as e:
            raise ImportError(
                textwrap.dedent(
                    f"""
                    Cannot import module "pyspark".

                    Since onETL v0.7.0 you should install package as follows:
                        pip install onetl[spark]

                    or inject PySpark to sys.path in some other way BEFORE creating {cls.__name__} instance.
                    """,
                ).strip(),
            ) from e

        refs["SparkSession"] = SparkSession
        return refs

    def _log_parameters(self):
        log.info("|Spark| Using connection parameters:")
        log_with_indent("type = %s", self.__class__.__name__)
        parameters = self.dict(by_alias=True, exclude_none=True, exclude={"spark"})
        for attr, value in sorted(parameters.items()):
            log_with_indent("%s = %r", attr, value)
