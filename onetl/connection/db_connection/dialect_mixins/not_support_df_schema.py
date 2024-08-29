# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class NotSupportDFSchema:
    connection: BaseDBConnection

    def validate_df_schema(
        self,
        df_schema: Any,
    ) -> None:
        if df_schema:
            raise ValueError(f"'df_schema' parameter is not supported by {self.connection.__class__.__name__}")
