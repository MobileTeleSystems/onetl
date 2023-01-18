#  Copyright 2022 MTS (Mobile Telesystems)
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
from onetl.impl import FrozenModel
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = getLogger(__name__)


class DBConnection(BaseDBConnection, FrozenModel):

    # TODO:(@dypedchenk) Create abstract class for engine. Engine uses pyhive session or Engine uses pyspark session
    spark: SparkSession = Field(repr=False)

    _check_query: ClassVar[str] = "SELECT 1"
    _compare_statements: ClassVar[Dict[Callable, str]] = {
        operator.ge: "{} >= {}",
        operator.gt: "{} > {}",
        operator.le: "{} <= {}",
        operator.lt: "{} < {}",
        operator.eq: "{} == {}",
        operator.ne: "{} != {}",
    }

    def expression_with_alias(self, expression: str, alias: str) -> str:
        return f"{expression} AS {alias}"

    def get_compare_statement(self, comparator: Callable, arg1: Any, arg2: Any) -> str:
        template = self._compare_statements[comparator]
        return template.format(arg1, self._get_value_sql(arg2))

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
