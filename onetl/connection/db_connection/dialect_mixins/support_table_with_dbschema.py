from __future__ import annotations

from etl_entities import Table

from onetl.base import BaseDBConnection


class SupportTableWithDBSchema:
    @classmethod
    def validate_name(cls, connection: BaseDBConnection, value: Table) -> Table:
        if value.db is None:
            # Same error text as in etl_entites.Table value error.
            raise ValueError(
                f"Table name should be passed in `schema.name` format, got '{value}'",
            )

        return value
