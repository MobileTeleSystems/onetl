# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from onetl.base import BaseDBConnection


class NotSupportWhere:
    connection: "BaseDBConnection"

    def validate_where(
        self,
        where: Any,
    ) -> None:
        if where is not None:
            msg = f"'where' parameter is not supported by {self.connection.__class__.__name__}"
            raise ValueError(msg)
