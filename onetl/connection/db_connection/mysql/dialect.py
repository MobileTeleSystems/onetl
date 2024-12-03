# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date, datetime

from onetl.connection.db_connection.jdbc_connection import JDBCDialect


class MySQLDialect(JDBCDialect):
    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str:
        # MD5 is the fastest hash function https://stackoverflow.com/a/3118889/23601543
        # But it returns 32 char string (128 bit), which we need to convert to integer
        return f"CAST(CONV(RIGHT(MD5({partition_column}), 16), 16, 10) AS UNSIGNED) % {num_partitions}"

    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str:
        # Return positive value even for negative input
        return f"ABS({partition_column} % {num_partitions})"

    def escape_column(self, value: str) -> str:
        return f"`{value}`"

    def _serialize_datetime(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"STR_TO_DATE('{result}', '%Y-%m-%d %H:%i:%s.%f')"

    def _serialize_date(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"STR_TO_DATE('{result}', '%Y-%m-%d')"
