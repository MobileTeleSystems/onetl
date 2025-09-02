# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from onetl.connection.db_connection.db_connection import DBDialect
from onetl.connection.db_connection.dialect_mixins import (
    NotSupportDFSchema,
    SupportColumns,
    SupportHintStr,
    SupportHWMExpressionStr,
    SupportNameWithSchemaOnly,
    SupportWhereStr,
)


class HiveDialect(  # noqa: WPS215
    SupportNameWithSchemaOnly,
    SupportColumns,
    NotSupportDFSchema,
    SupportWhereStr,
    SupportHintStr,
    SupportHWMExpressionStr,
    DBDialect,
):
    def escape_column(self, value: str) -> str:
        return f"`{value}`"
