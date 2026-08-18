# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import date, datetime

from onetl.connection.db_connection.db_connection import DBDialect
from onetl.connection.db_connection.dialect_mixins import (
    NotSupportDFSchema,
    NotSupportHint,
    SupportColumns,
    SupportHWMExpressionStr,
    SupportNameWithSchemaOnly,
    SupportWhereStr,
)


class GreenplumDialect(
    SupportNameWithSchemaOnly,
    SupportColumns,
    NotSupportDFSchema,
    SupportWhereStr,
    NotSupportHint,
    SupportHWMExpressionStr,
    DBDialect,
):
    def _serialize_datetime(self, value: datetime) -> str:
        result = value.isoformat()
        return f"cast('{result}' as timestamp)"

    def _serialize_date(self, value: date) -> str:
        result = value.isoformat()
        return f"cast('{result}' as date)"
