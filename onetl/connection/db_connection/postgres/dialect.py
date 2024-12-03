# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date, datetime

from onetl.connection.db_connection.dialect_mixins import NotSupportHint
from onetl.connection.db_connection.jdbc_connection import JDBCDialect


class PostgresDialect(NotSupportHint, JDBCDialect):
    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str:
        # hashtext is about 3-5 times faster than MD5 (tested locally)
        # https://postgrespro.com/list/thread-id/1506406
        return f"abs(hashtext({partition_column}::text)) % {num_partitions}"

    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str:
        # Return positive value even for negative input
        return f"abs({partition_column} % {num_partitions})"

    def _serialize_datetime(self, value: datetime) -> str:
        result = value.isoformat()
        return f"'{result}'::timestamp"

    def _serialize_date(self, value: date) -> str:
        result = value.isoformat()
        return f"'{result}'::date"
