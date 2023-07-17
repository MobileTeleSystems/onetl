from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportWhereNone:
    @classmethod
    def validate_where(
        cls,
        connection: BaseDBConnection,
        where: Any,
    ) -> None:
        if where is not None:
            raise TypeError(f"'where' parameter is not supported by {connection.__class__.__name__}")
