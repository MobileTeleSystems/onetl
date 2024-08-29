# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class NotSupportColumns:
    connection: BaseDBConnection

    def validate_columns(
        self,
        columns: Any,
    ) -> None:
        if columns is not None:
            raise ValueError(f"'columns' parameter is not supported by {self.connection.__class__.__name__}")
