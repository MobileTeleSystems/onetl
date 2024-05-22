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


class TeradataReadOptions(JDBCReadOptions):
    pass


class TeradataWriteOptions(JDBCWriteOptions):
    pass


class TeradataSQLOptions(JDBCSQLOptions):
    pass


class TeradataFetchOptions(JDBCFetchOptions):
    pass


class TeradataExecuteOptions(JDBCExecuteOptions):
    pass
