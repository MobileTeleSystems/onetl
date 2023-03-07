from __future__ import annotations

from typing import Any

from onetl.base import BaseDBConnection


class SupportHintStr:
    @classmethod
    def validate_hint(
        cls,
        connection: BaseDBConnection,
        hint: Any,
    ) -> str | None:
        if hint is None:
            return None

        if not isinstance(hint, str):
            raise TypeError(
                f"{connection.__class__.__name__} requires 'hint' parameter type to be 'str', "
                f"got {hint.__class__.__name__!r}",
            )

        return hint
