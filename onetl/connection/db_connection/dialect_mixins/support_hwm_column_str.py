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
        if not isinstance(hwm.entity, str):
            raise ValueError(
                f"{connection.__class__.__name__} requires 'hwm.column' parameter type to be 'str', "
                f"got {type(hwm.entity)}",
            )

        # call the base logic that validates hwm.expression (may be defined in separate mixins)
        super().validate_hwm(connection, hwm)

        return hwm
