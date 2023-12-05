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
