from __future__ import annotations

from etl_entities import Table

from onetl.base import BaseDBConnection


class SupportNameWithSchemaOnly:
    @classmethod
    def validate_name(cls, connection: BaseDBConnection, value: Table) -> Table:
        if value.name.count(".") != 1:
            raise ValueError(
                f"Name should be passed in `schema.name` format, got '{value}'",
            )

        return value
