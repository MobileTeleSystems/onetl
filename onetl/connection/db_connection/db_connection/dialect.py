# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from onetl.base import BaseDBDialect
from onetl.hwm import Edge, Window
from onetl.hwm.store import SparkTypeToHWM

if TYPE_CHECKING:
    from etl_entities.hwm import HWM
    from pyspark.sql.types import StructField


class DBDialect(BaseDBDialect):
    def detect_hwm_class(self, field: StructField) -> type[HWM] | None:
        return SparkTypeToHWM.get(field.dataType.typeName())  # type: ignore

    def get_sql_query(
        self,
        table: str,
        columns: list[str] | None = None,
        where: str | list[str] | None = None,
        hint: str | None = None,
        limit: int | None = None,
        compact: bool = False,
    ) -> str:
        """
        Generates a SQL query using input arguments
        """

        if compact:
            indent = " "
        else:
            indent = os.linesep + " " * 7

        hint = f" /*+ {hint} */" if hint else ""

        columns_str = indent + "*"
        if columns:
            columns_str = indent + f",{indent}".join(column for column in columns)

        where = where or []
        if isinstance(where, str):
            where = [where]

        if limit == 0:
            # LIMIT 0 not valid in some databases
            where = ["1 = 0"]

        where_clauses = []
        if len(where) == 1:
            where_clauses.append("WHERE" + indent + where[0])
        else:
            for i, item in enumerate(where):
                directive = "WHERE" if i == 0 else "  AND"
                where_clauses.append(directive + indent + f"({item})")

        query_parts = [
            f"SELECT{hint}{columns_str}",
            f"FROM{indent}{table}",
            *where_clauses,
            f"LIMIT{indent}{limit}" if limit else "",
        ]

        return os.linesep.join(filter(None, query_parts)).strip()

    def apply_window(
        self,
        condition: Any,
        window: Window | None = None,
    ) -> Any:
        conditions = [
            condition,
            self._edge_to_where(window.expression, window.start_from, position="start") if window else None,
            self._edge_to_where(window.expression, window.stop_at, position="end") if window else None,
        ]
        return list(filter(None, conditions))

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

    def _edge_to_where(
        self,
        expression: str,
        edge: Edge,
        position: str,
    ) -> Any:
        if not edge.is_set():
            return None

        operators: dict[tuple[str, bool], str] = {
            ("start", True): ">=",
            ("start", False): "> ",
            ("end", True): "<=",
            ("end", False): "< ",
        }

        operator = operators[(position, edge.including)]
        value = self._serialize_value(edge.value)
        return f"{expression} {operator} {value}"

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
