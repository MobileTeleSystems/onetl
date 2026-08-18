# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from onetl.base import BaseDBConnection


class NotSupportDFSchema:
    connection: "BaseDBConnection"

    def validate_df_schema(
        self,
        df_schema: Any,
    ) -> None:
        if df_schema:
            msg = f"'df_schema' parameter is not supported by {self.connection.__class__.__name__}"
            raise ValueError(msg)
