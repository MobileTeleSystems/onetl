# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0


from __future__ import annotations

try:
    from pydantic.v1 import AnyUrl, ConstrainedStr, parse_obj_as
except (ImportError, AttributeError):
    from pydantic import (  # type: ignore[no-redef, assignment]
        AnyUrl,
        ConstrainedStr,
        parse_obj_as,
    )


class Host(ConstrainedStr):
    """Generic host representation"""

    min_length = 1

    @classmethod
    def validate(cls, value: str) -> str:
        url = parse_obj_as(AnyUrl, f"http://{value}")  # NOSONAR
        if url.host != value:
            msg = f"Invalid host {value}"
            raise ValueError(msg)

        return value
