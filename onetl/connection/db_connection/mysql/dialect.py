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
