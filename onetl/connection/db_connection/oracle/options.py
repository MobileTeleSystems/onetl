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


class OracleReadOptions(JDBCReadOptions):
    pass


class OracleWriteOptions(JDBCWriteOptions):
    pass


class OracleSQLOptions(JDBCSQLOptions):
    pass


class OracleFetchOptions(JDBCFetchOptions):
    pass


class OracleExecuteOptions(JDBCExecuteOptions):
    pass
