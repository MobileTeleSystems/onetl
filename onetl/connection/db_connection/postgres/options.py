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


class PostgresReadOptions(JDBCReadOptions):
    __doc__ = JDBCReadOptions.__doc__.replace("SomeDB", "Postgres")  # type: ignore[assignment, union-attr]


class PostgresWriteOptions(JDBCWriteOptions):
    __doc__ = JDBCWriteOptions.__doc__.replace("SomeDB", "Postgres")  # type: ignore[assignment, union-attr]


class PostgresSQLOptions(JDBCSQLOptions):
    __doc__ = JDBCSQLOptions.__doc__.replace("SomeDB", "Postgres")  # type: ignore[assignment, union-attr]


class PostgresFetchOptions(JDBCFetchOptions):
    __doc__ = JDBCFetchOptions.__doc__.replace("SomeDB", "Postgres")  # type: ignore[assignment, union-attr]


class PostgresExecuteOptions(JDBCExecuteOptions):
    __doc__ = JDBCExecuteOptions.__doc__.replace("SomeDB", "Postgres")  # type: ignore[assignment, union-attr]
