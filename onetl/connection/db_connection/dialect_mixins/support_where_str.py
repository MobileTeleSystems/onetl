# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from onetl.base import BaseDBConnection


class SupportWhereStr:
    connection: "BaseDBConnection"

    def validate_where(
        self,
        where: Any,
    ) -> str | None:
        if where is None:
            return None

        if not isinstance(where, str):
            msg = (
                f"{self.connection.__class__.__name__} requires 'where' parameter type to be 'str', "
                f"got {where.__class__.__name__!r}"
            )
            raise ValueError(msg)  # noqa: TRY004

        return where
