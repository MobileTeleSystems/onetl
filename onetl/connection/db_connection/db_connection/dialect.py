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
from datetime import date, datetime
from typing import Any, Callable, ClassVar, Dict

from etl_entities.hwm import ColumnHWM

from onetl.base import BaseDBDialect
from onetl.hwm import Statement
from onetl.hwm.store import SparkTypeToHWM


class DBDialect(BaseDBDialect):
    _compare_statements: ClassVar[Dict[Callable, str]] = {
        operator.ge: "{} >= {}",
        operator.gt: "{} > {}",
        operator.le: "{} <= {}",
        operator.lt: "{} < {}",
        operator.eq: "{} == {}",
        operator.ne: "{} != {}",
    }

    def detect_hwm_class(self, hwm_column_type: str) -> type[ColumnHWM]:
        return SparkTypeToHWM.get(hwm_column_type)  # type: ignore

    def escape_column(self, value: str) -> str:
        return f'"{value}"'

    def aliased(self, expression: str, alias: str) -> str:
        return f"{expression} AS {alias}"

    def get_max_value(self, value: Any) -> str:
        """
        Generate `MAX(value)` clause for given value
        """
        result = self._serialize_value(value)
        return f"MAX({result})"

    def get_min_value(self, value: Any) -> str:
        """
        Generate `MIN(value)` clause for given value
        """
        result = self._serialize_value(value)
        return f"MIN({result})"

    def condition_assembler(
        self,
        condition: Any,
        start_from: Statement | None,
        end_at: Statement | None,
    ) -> Any:
        conditions = [condition]

        if start_from:
            condition1 = self._get_compare_statement(
                comparator=start_from.operator,
                arg1=start_from.expression,
                arg2=start_from.value,
            )
            conditions.append(condition1)

        if end_at:
            condition2 = self._get_compare_statement(
                comparator=end_at.operator,
                arg1=end_at.expression,
                arg2=end_at.value,
            )
            conditions.append(condition2)

        result: list[Any] = list(filter(None, conditions))
        if not result:
            return None
        return self._merge_conditions(result)

    def _get_compare_statement(self, comparator: Callable, arg1: Any, arg2: Any) -> Any:
        template = self._compare_statements[comparator]
        return template.format(arg1, self._serialize_value(arg2))

    def _merge_conditions(self, conditions: list[Any]) -> Any:
        if len(conditions) == 1:
            return conditions[0]

        return " AND ".join(f"({item})" for item in conditions)

    def _serialize_value(self, value: Any) -> str | int | dict:
        """
        Transform the value into an SQL Dialect-supported form.
        """

        if isinstance(value, datetime):
            return self._serialize_datetime(value)

        if isinstance(value, date):
            return self._serialize_date(value)

        return str(value)

    def _serialize_datetime(self, value: datetime) -> str:
        """
        Transform the datetime value into supported by SQL Dialect
        """
        result = value.isoformat()
        return repr(result)

    def _serialize_date(self, value: date) -> str:
        """
        Transform the date value into supported by SQL Dialect
        """
        result = value.isoformat()
        return repr(result)
