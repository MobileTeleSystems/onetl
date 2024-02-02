# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date, datetime

from onetl.connection.db_connection.jdbc_connection import JDBCDialect


class MySQLDialect(JDBCDialect):
    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str:
        return f"MOD(CONV(CONV(RIGHT(MD5({partition_column}), 16), 16, 2), 2, 10), {num_partitions})"

    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str:
        return f"MOD({partition_column}, {num_partitions})"

    def escape_column(self, value: str) -> str:
        return f"`{value}`"

    def _serialize_datetime(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"STR_TO_DATE('{result}', '%Y-%m-%d %H:%i:%s.%f')"

    def _serialize_date(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"STR_TO_DATE('{result}', '%Y-%m-%d')"
