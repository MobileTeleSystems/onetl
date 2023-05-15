from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportHWMExpressionStr:
    @classmethod
    def validate_hwm_expression(cls, connection: BaseDBConnection, value: Any) -> str | None:
        if value is None:
            return None

        if not isinstance(value, str):
            raise TypeError(
                f"{connection.__class__.__name__} requires 'hwm_expression' parameter type to be 'str', "
                f"got {value.__class__.__name__!r}",
            )

        return value
