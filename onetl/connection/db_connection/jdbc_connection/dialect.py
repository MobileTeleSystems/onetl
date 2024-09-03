# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

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


class JDBCDialect(  # noqa: WPS215
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
