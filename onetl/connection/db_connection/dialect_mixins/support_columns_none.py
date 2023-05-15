from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportColumnsNone:
    @classmethod
    def validate_columns(
        cls,
        connection: BaseDBConnection,
        columns: Any,
    ) -> None:
        if columns is not None:
            raise ValueError(f"'columns' parameter is not supported by {connection.__class__.__name__}")
