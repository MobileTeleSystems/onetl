# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportWhereStr:
    connection: BaseDBConnection

    def validate_where(
        self,
        where: Any,
    ) -> str | None:
        if where is None:
            return None

        if not isinstance(where, str):
            raise TypeError(
                f"{self.connection.__class__.__name__} requires 'where' parameter type to be 'str', "
                f"got {where.__class__.__name__!r}",
            )

        return where
