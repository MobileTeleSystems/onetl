from __future__ import annotations

from etl_entities import Table

from onetl.base import BaseDBConnection


class SupportTableWithoutDBSchema:
    @classmethod
    def validate_table(cls, connection: BaseDBConnection, value: Table) -> Table:
        if value.db is not None:
            raise ValueError(
                f"{connection.__class__.__name__} Table name should be passed in `table_name` format "
                f"(not `schema.table`), got '{value}'",
            )
        return value
