from __future__ import annotations

from etl_entities.source import Table


class SupportNameAny:
    def validate_name(self, value: Table) -> Table:
        return value
