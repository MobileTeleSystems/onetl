from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class NotSupportHint:
    connection: BaseDBConnection

    def validate_hint(
        self,
        hint: Any,
    ) -> None:
        if hint is not None:
            raise TypeError(f"'hint' parameter is not supported by {self.connection.__class__.__name__}")
