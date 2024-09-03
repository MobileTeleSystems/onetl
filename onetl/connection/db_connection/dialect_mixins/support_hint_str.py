# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportHintStr:
    connection: BaseDBConnection

    def validate_hint(
        self,
        hint: Any,
    ) -> str | None:
        if hint is None:
            return None

        if not isinstance(hint, str):
            raise TypeError(
                f"{self.connection.__class__.__name__} requires 'hint' parameter type to be 'str', "
                f"got {hint.__class__.__name__!r}",
            )

        return hint
