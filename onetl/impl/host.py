# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0


from __future__ import annotations

from typing import Annotated

from pydantic import (
    AfterValidator,
    AnyUrl,
    StringConstraints,
    TypeAdapter,
)

AnyUrlTypeAdapter = TypeAdapter(AnyUrl)


def validate(value: str) -> str:
    url = AnyUrlTypeAdapter.validate_python(f"http://{value}")
    if url.host != value:
        msg = f"Invalid host {value}"
        raise ValueError(msg)

    return value


Host = Annotated[str, StringConstraints(min_length=1), AfterValidator(validate)]
