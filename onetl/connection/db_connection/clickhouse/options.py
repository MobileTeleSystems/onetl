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


class ClickhouseReadOptions(JDBCReadOptions):
    pass


class ClickhouseWriteOptions(JDBCWriteOptions):
    pass


class ClickhouseSQLOptions(JDBCSQLOptions):
    pass


class ClickhouseFetchOptions(JDBCFetchOptions):
    pass


class ClickhouseExecuteOptions(JDBCExecuteOptions):
    pass
