# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from typing_extensions import Protocol, runtime_checkable

from onetl.hwm.window import Window


@runtime_checkable
class ContainsGetMinMaxValues(Protocol):
    """
    Protocol for objects containing ``get_min_max_values`` method
    """

    def get_min_max_values(
        self,
        source: str,
        window: Window,
        hint: Any | None = None,
        where: Any | None = None,
    ) -> tuple[Any, Any]:
        """
        Get MIN and MAX values for the column in the source. |support_hooks|
        """
