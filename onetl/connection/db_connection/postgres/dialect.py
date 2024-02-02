# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date, datetime

from onetl.connection.db_connection.dialect_mixins import NotSupportHint
from onetl.connection.db_connection.jdbc_connection import JDBCDialect


class PostgresDialect(NotSupportHint, JDBCDialect):
    # https://stackoverflow.com/a/9812029
    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str:
        return f"('x'||right(md5('{partition_column}'), 16))::bit(32)::bigint % {num_partitions}"

    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str:
        return f"{partition_column} % {num_partitions}"

    def _serialize_datetime(self, value: datetime) -> str:
        result = value.isoformat()
        return f"'{result}'::timestamp"

    def _serialize_date(self, value: date) -> str:
        result = value.isoformat()
        return f"'{result}'::date"
