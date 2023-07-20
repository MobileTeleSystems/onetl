from __future__ import annotations

from onetl.base import BaseDBConnection


class SupportHWMColumnStr:
    @classmethod
    def validate_hwm_column(
        cls,
        connection: BaseDBConnection,
        hwm_column: str | None,
    ) -> str | None:
        if not isinstance(hwm_column, str):
            raise ValueError(
                f"{connection.__class__.__name__} requires 'hwm_column' parameter type to be 'str', "
                f"got {type(hwm_column)}",
            )

        return hwm_column
