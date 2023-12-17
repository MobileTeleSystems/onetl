from __future__ import annotations

from typing import Any


class SupportColumns:
    def validate_columns(
        self,
        columns: Any,
    ) -> list[str] | None:
        if columns is None:
            return ["*"]

        return list(columns)
