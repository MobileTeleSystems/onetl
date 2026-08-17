# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Protocol, runtime_checkable

from onetl.hwm.window import Window


@runtime_checkable
class ContainsGetMinMaxValues(Protocol):
    """
    Protocol for objects containing `get_min_max_values` method
    """

    def get_min_max_values(
        self,
        source: str,
        window: Window,
        hint: Any | None = None,
        where: Any | None = None,
    ) -> tuple[Any, Any]:
        """
        Get MIN and MAX values for the column in the source. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]
        """
