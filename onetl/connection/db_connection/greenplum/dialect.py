#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from datetime import date, datetime

from onetl.connection.db_connection.db_connection import DBDialect
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsList,
    SupportDfSchemaNone,
    SupportHintNone,
    SupportHWMColumnStr,
    SupportHWMExpressionStr,
    SupportNameWithSchemaOnly,
    SupportWhereStr,
)


class GreenplumDialect(  # noqa: WPS215
    SupportNameWithSchemaOnly,
    SupportColumnsList,
    SupportDfSchemaNone,
    SupportWhereStr,
    SupportHintNone,
    SupportHWMExpressionStr,
    SupportHWMColumnStr,
    DBDialect,
):
    @classmethod
    def _get_datetime_value_sql(cls, value: datetime) -> str:
        result = value.isoformat()
        return f"cast('{result}' as timestamp)"

    @classmethod
    def _get_date_value_sql(cls, value: date) -> str:
        result = value.isoformat()
        return f"cast('{result}' as date)"
