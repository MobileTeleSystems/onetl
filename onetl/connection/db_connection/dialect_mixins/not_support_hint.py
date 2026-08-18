# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from onetl.base import BaseDBConnection


class NotSupportHint:
    connection: "BaseDBConnection"

    def validate_hint(
        self,
        hint: Any,
    ) -> None:
        if hint is not None:
            msg = f"'hint' parameter is not supported by {self.connection.__class__.__name__}"
            raise ValueError(msg)
