# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date, datetime

from onetl.connection.db_connection.jdbc_connection import JDBCDialect


class MSSQLDialect(JDBCDialect):
    # https://docs.microsoft.com/ru-ru/sql/t-sql/functions/hashbytes-transact-sql?view=sql-server-ver16
    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str:
        return f"CONVERT(BIGINT, HASHBYTES ( 'SHA' , {partition_column} )) % {num_partitions}"

    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str:
        return f"{partition_column} % {num_partitions}"

    def get_sql_query(
        self,
        table: str,
        columns: list[str] | None = None,
        where: str | list[str] | None = None,
        hint: str | None = None,
        limit: int | None = None,
        compact: bool = False,
    ) -> str:
        query = super().get_sql_query(
            table=table,
            columns=columns,
            where=where,
            hint=hint,
            limit=0 if limit == 0 else None,
            compact=compact,
        )
        # MSSQL-specific handling for the LIMIT clause using TOP
        if limit is not None and limit > 0:
            query = query.replace("SELECT", f"SELECT TOP {limit}", 1)

        return query

    def _serialize_datetime(self, value: datetime) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS datetime2)"

    def _serialize_date(self, value: date) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS date)"
