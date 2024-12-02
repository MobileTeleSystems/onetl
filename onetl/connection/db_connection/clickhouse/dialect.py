# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from onetl.connection.db_connection.jdbc_connection import JDBCDialect


class ClickhouseDialect(JDBCDialect):
    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str:
        # SipHash is 3 times faster thah MD5
        # https://clickhouse.com/docs/en/sql-reference/functions/hash-functions#siphash64
        return f"sipHash64({partition_column}) % {num_partitions}"

    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str:
        # Return positive value even for negative input.
        # Don't use positiveModulo as it is 4-5 times slower:
        # https://clickhouse.com/docs/en/sql-reference/functions/arithmetic-functions#positivemoduloa-b
        return f"abs({partition_column} % {num_partitions})"

    def get_max_value(self, value: Any) -> str:
        # Max function in Clickhouse returns 0 instead of NULL for empty table
        result = self._serialize_value(value)
        return f"maxOrNull({result})"

    def get_min_value(self, value: Any) -> str:
        # Min function in Clickhouse returns 0 instead of NULL for empty table
        result = self._serialize_value(value)
        return f"minOrNull({result})"

    def _serialize_datetime(self, value: datetime) -> str:
        # this requires at least Clickhouse 21.1, see:
        # https://github.com/ClickHouse/ClickHouse/issues/16655
        result = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"toDateTime64('{result}', 6)"

    def _serialize_date(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"toDate('{result}')"
