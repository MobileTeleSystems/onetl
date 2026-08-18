# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from onetl.base import BaseDBConnection


class SupportHintStr:
    connection: "BaseDBConnection"

    def validate_hint(
        self,
        hint: Any,
    ) -> str | None:
        if hint is None:
            return None

        if not isinstance(hint, str):
            msg = (
                f"{self.connection.__class__.__name__} requires 'hint' parameter type to be 'str', "
                f"got {hint.__class__.__name__!r}"
            )
            raise ValueError(msg)  # noqa: TRY004

        return hint
