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
        return super().get_sql_query(
            table=table,
            columns=new_columns,
            where=where,
            hint=hint,
            limit=limit,
            compact=compact,
        )

    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str:
        return f"ora_hash({partition_column}, {num_partitions})"

    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str:
        return f"MOD({partition_column}, {num_partitions})"

    def _serialize_datetime(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"TO_DATE('{result}', 'YYYY-MM-DD HH24:MI:SS')"

    def _serialize_date(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"TO_DATE('{result}', 'YYYY-MM-DD')"
