# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class NotSupportWhere:
    connection: BaseDBConnection

    def validate_where(
        self,
        where: Any,
    ) -> None:
        if where is not None:
            raise TypeError(f"'where' parameter is not supported by {self.connection.__class__.__name__}")
