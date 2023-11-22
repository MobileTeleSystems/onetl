from __future__ import annotations

from etl_entities.hwm import HWM

from onetl.base import BaseDBConnection


class SupportHWMColumnStr:
    @classmethod
    def validate_hwm(
        cls,
        connection: BaseDBConnection,
        hwm: HWM,
    ) -> HWM:
        hwm_column = hwm.entity

        if not isinstance(hwm_column, str):
            raise ValueError(
                f"{connection.__class__.__name__} requires 'hwm.column' parameter type to be 'str', "
                f"got {type(hwm_column)}",
            )

        return hwm
