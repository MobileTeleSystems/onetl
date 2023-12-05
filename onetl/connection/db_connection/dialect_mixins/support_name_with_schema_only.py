from __future__ import annotations

from etl_entities.source import Table


class SupportNameWithSchemaOnly:
    def validate_name(cls, value: Table) -> Table:
        if value.name.count(".") != 1:
            raise ValueError(
                f"Name should be passed in `schema.name` format, got '{value}'",
            )

        return value
