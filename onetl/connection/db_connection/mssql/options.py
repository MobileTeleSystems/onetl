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


class MSSQLReadOptions(JDBCReadOptions):
    __doc__ = JDBCReadOptions.__doc__.replace("SomeDB", "MSSQL")  # type: ignore[assignment, union-attr]


class MSSQLWriteOptions(JDBCWriteOptions):
    __doc__ = JDBCWriteOptions.__doc__.replace("SomeDB", "MSSQL")  # type: ignore[assignment, union-attr]


class MSSQLSQLOptions(JDBCSQLOptions):
    __doc__ = JDBCSQLOptions.__doc__.replace("SomeDB", "MSSQL")  # type: ignore[assignment, union-attr]


class MSSQLFetchOptions(JDBCFetchOptions):
    __doc__ = JDBCFetchOptions.__doc__.replace("SomeDB", "MSSQL")  # type: ignore[assignment, union-attr]


class MSSQLExecuteOptions(JDBCExecuteOptions):
    __doc__ = JDBCExecuteOptions.__doc__.replace("SomeDB", "MSSQL")  # type: ignore[assignment, union-attr]
