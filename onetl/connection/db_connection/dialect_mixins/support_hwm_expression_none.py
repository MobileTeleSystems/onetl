from __future__ import annotations

from etl_entities.hwm import HWM

from onetl.base import BaseDBConnection


class SupportHWMExpressionNone:
    @classmethod
    def validate_hwm_expression(cls, connection: BaseDBConnection, hwm: HWM) -> HWM | None:
        if hwm.expression is not None:
            raise ValueError(
                f"'hwm.expression' parameter is not supported by {connection.__class__.__name__}",
            )
        return hwm
