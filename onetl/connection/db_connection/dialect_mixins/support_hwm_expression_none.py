from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportHWMExpressionNone:
    @classmethod
    def _validate_hwm_expression(cls, connection: BaseDBConnection, value: Any) -> str | None:
        if value is not None:
            raise ValueError(
                f"|{connection.__class__.__name__}| You can't pass the 'hwm_expression' parameter",
            )
        return value
