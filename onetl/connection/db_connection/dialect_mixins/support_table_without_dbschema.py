from __future__ import annotations

from etl_entities import Table

from onetl.base import BaseDBConnection


class SupportTableWithoutDBSchema:
    @classmethod
    def validate_table(cls, connection: BaseDBConnection, value: Table) -> Table:
        if value.db is not None:
            raise ValueError(
                f"Table name should be passed in `table_name` format (not `schema.table`), got '{value}'",
            )
        return value
