from __future__ import annotations

from etl_entities.hwm import HWM

from onetl.base import BaseDBConnection


class SupportHWMExpressionStr:
    @classmethod
    def validate_hwm_expression(cls, connection: BaseDBConnection, hwm: HWM) -> HWM | None:
        hwm_expression = hwm.expression
        if hwm_expression is None:
            return None

        if not isinstance(hwm_expression, str):
            raise TypeError(
                f"{connection.__class__.__name__} requires 'hwm.expression' parameter type to be 'str', "
                f"got {hwm_expression.__class__.__name__!r}",
            )

        return hwm
