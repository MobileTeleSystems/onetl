from __future__ import annotations

from etl_entities import Table

from onetl.base import BaseDBConnection


class SupportTableWithDBSchema:
    @classmethod
    def _validate_table(cls, connection: BaseDBConnection, value: Table) -> Table:
        if value.db is None:
            # Same error text as in etl_entites.Table value error.
            raise ValueError(
                f"{connection.__class__.__name__} Table name should be passed in `schema.name` format, "
                f"got '{value}'",
            )

        return value
