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


class MySQLDialect(JDBCDialect):
    @classmethod
    def _escape_column(cls, value: str) -> str:
        return f"`{value}`"

    @classmethod
    def _get_datetime_value_sql(cls, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"STR_TO_DATE('{result}', '%Y-%m-%d %H:%i:%s.%f')"

    @classmethod
    def _get_date_value_sql(cls, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"STR_TO_DATE('{result}', '%Y-%m-%d')"

    @classmethod
    def _get_partition_column_hash(cls, partition_column: str, num_partitions: int) -> str:
        return f"MOD(CONV(CONV(RIGHT(MD5({partition_column}), 16),16, 2), 2, 10), {num_partitions})"

    @classmethod
    def _get_partition_column_mod(cls, partition_column: str, num_partitions: int) -> str:
        return f"MOD({partition_column}, {num_partitions})"
