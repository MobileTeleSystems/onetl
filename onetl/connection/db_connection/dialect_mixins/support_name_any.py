from __future__ import annotations

from etl_entities import Table

from onetl.base import BaseDBConnection


class SupportNameAny:
    @classmethod
    def validate_name(cls, connection: BaseDBConnection, value: Table) -> Table:
        return value
