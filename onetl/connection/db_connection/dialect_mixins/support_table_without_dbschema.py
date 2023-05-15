from __future__ import annotations

from etl_entities import Table

from onetl.base import BaseDBConnection


class SupportTableWithoutDBSchema:
    @classmethod
    def validate_name(cls, connection: BaseDBConnection, value: Table) -> Table:
        if value.db is not None:
            raise ValueError(
                f"Table name should be passed in `mytable` format (not `myschema.mytable`), got '{value}'",
            )
        return value
