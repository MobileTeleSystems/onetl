# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0

from onetl.connection.db_connection.jdbc_connection.options import (
    JDBCReadOptions,
    JDBCSQLOptions,
    JDBCWriteOptions,
)
from onetl.connection.db_connection.jdbc_mixin.options import (
    JDBCExecuteOptions,
    JDBCFetchOptions,
)


class PostgresReadOptions(JDBCReadOptions):
    pass


class PostgresWriteOptions(JDBCWriteOptions):
    pass


class PostgresSQLOptions(JDBCSQLOptions):
    pass


class PostgresFetchOptions(JDBCFetchOptions):
    pass


class PostgresExecuteOptions(JDBCExecuteOptions):
    pass
