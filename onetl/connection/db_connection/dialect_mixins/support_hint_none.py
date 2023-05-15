from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportHintNone:
    @classmethod
    def validate_hint(
        cls,
        connection: BaseDBConnection,
        hint: Any,
    ) -> None:
        if hint is not None:
            raise TypeError(f"'hint' parameter is not supported by {connection.__class__.__name__}")
