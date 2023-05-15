from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportHWMExpressionNone:
    @classmethod
    def validate_hwm_expression(cls, connection: BaseDBConnection, value: Any) -> str | None:
        if value is not None:
            raise ValueError(
                f"'hwm_expression' parameter is not supported by {connection.__class__.__name__}",
            )
        return value
