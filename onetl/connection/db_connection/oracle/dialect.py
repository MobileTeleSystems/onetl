# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date, datetime

from onetl.connection.db_connection.jdbc_connection import JDBCDialect


class OracleDialect(JDBCDialect):
    def get_sql_query(
        self,
        table: str,
        columns: list[str] | None = None,
        where: str | list[str] | None = None,
        hint: str | None = None,
        limit: int | None = None,
        compact: bool = False,
    ) -> str:
        # https://stackoverflow.com/questions/27965130/how-to-select-column-from-table-in-oracle
        new_columns = columns or ["*"]
        if len(new_columns) > 1:
            new_columns = [table + ".*" if column.strip() == "*" else column for column in new_columns]

        where = where or []
        if isinstance(where, str):
            where = [where]

        if limit is not None:
            if limit == 0:
                where = ["1=0"]
            else:
                # Oracle does not support LIMIT
                where.append(f"ROWNUM <= {limit}")

        return super().get_sql_query(
            table=table,
            columns=new_columns,
            where=where,
            hint=hint,
            limit=None,
            compact=compact,
        )

    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str:
        # ora_hash returns values from 0 to N including N.
        # Balancing N+1 splits to N partitions leads to data skew in last partition.
        return f"ora_hash({partition_column}, {num_partitions - 1})"

    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str:
        # Return positive value even for negative input
        return f"ABS(MOD({partition_column}, {num_partitions}))"

    def _serialize_datetime(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"TO_DATE('{result}', 'YYYY-MM-DD HH24:MI:SS')"

    def _serialize_date(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"TO_DATE('{result}', 'YYYY-MM-DD')"
