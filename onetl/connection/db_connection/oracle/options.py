# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
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
    __doc__ = JDBCReadOptions.__doc__  # type: ignore[assignment]


class OracleWriteOptions(JDBCWriteOptions):
    __doc__ = JDBCWriteOptions.__doc__  # type: ignore[assignment]


class OracleSQLOptions(JDBCSQLOptions):
    __doc__ = JDBCSQLOptions.__doc__  # type: ignore[assignment]


class OracleFetchOptions(JDBCFetchOptions):
    __doc__ = JDBCFetchOptions.__doc__  # type: ignore[assignment]


class OracleExecuteOptions(JDBCExecuteOptions):
    __doc__ = JDBCExecuteOptions.__doc__  # type: ignore[assignment]
