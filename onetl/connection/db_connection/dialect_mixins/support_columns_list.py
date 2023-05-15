from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportColumnsList:
    @classmethod
    def validate_columns(
        cls,
        connection: BaseDBConnection,
        columns: Any,
    ) -> list[str] | None:
        if columns is None:
            return ["*"]

        return list(columns)
