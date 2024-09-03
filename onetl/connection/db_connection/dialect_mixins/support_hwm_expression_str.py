# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from etl_entities.hwm import HWM

from onetl.base import BaseDBConnection


class SupportHWMExpressionStr:
    connection: BaseDBConnection

    def validate_hwm(self, hwm: HWM | None) -> HWM | None:
        if not hwm or hwm.expression is None:
            return hwm

        if not isinstance(hwm.expression, str):
            raise TypeError(
                f"{self.connection.__class__.__name__} requires 'hwm.expression' parameter type to be 'str', "
                f"got {hwm.expression.__class__.__name__!r}",
            )

        return hwm
