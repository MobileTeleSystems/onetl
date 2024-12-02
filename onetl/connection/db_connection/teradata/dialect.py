# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date, datetime

from onetl.connection.db_connection.jdbc_connection import JDBCDialect


class TeradataDialect(JDBCDialect):
    # https://docs.teradata.com/r/w4DJnG9u9GdDlXzsTXyItA/lkaegQT4wAakj~K_ZmW1Dg
    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str:
        return f"HASHAMP(HASHBUCKET(HASHROW({partition_column}))) mod {num_partitions}"

    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str:
        # Return positive value even for negative input
        return f"ABS({partition_column} mod {num_partitions})"

    def _serialize_datetime(self, value: datetime) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS TIMESTAMP)"

    def _serialize_date(self, value: date) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS DATE)"
