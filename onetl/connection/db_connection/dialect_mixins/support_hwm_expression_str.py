# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING

from etl_entities.hwm import HWM

if TYPE_CHECKING:
    from onetl.base import BaseDBConnection


class SupportHWMExpressionStr:
    connection: "BaseDBConnection"

    def validate_hwm(self, hwm: HWM | None) -> HWM | None:
        if not hwm or hwm.expression is None:
            return hwm

        if not isinstance(hwm.expression, str):
            msg = (
                f"{self.connection.__class__.__name__} requires 'hwm.expression' parameter type to be 'str', "
                f"got {hwm.expression.__class__.__name__!r}"
            )
            raise ValueError(msg)  # noqa: TRY004

        return hwm
