from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportWhereStr:
    @classmethod
    def validate_where(
        cls,
        connection: BaseDBConnection,
        where: Any,
    ) -> str | None:
        if where is None:
            return None

        if not isinstance(where, str):
            raise TypeError(
                f"{connection.__class__.__name__} requires 'where' parameter type to be 'str', "
                f"got {where.__class__.__name__!r}",
            )

        return where
