# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import abstractmethod

from onetl.connection.db_connection.db_connection import DBDialect
from onetl.connection.db_connection.dialect_mixins import (
    NotSupportDFSchema,
    SupportColumns,
    SupportHintStr,
    SupportHWMExpressionStr,
    SupportNameWithSchemaOnly,
    SupportWhereStr,
)


class JDBCDialect(
    SupportNameWithSchemaOnly,
    SupportColumns,
    NotSupportDFSchema,
    SupportWhereStr,
    SupportHintStr,
    SupportHWMExpressionStr,
    DBDialect,
):
    @abstractmethod
    def get_partition_column_hash(self, partition_column: str, num_partitions: int) -> str: ...

    @abstractmethod
    def get_partition_column_mod(self, partition_column: str, num_partitions: int) -> str: ...
