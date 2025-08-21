# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations


class SupportNameWithMultipleParts:
    def validate_name(self, value: str) -> str:
        if "." not in value:
            raise ValueError(
                f"Name should be passed in `schema.table`, `catalog.schema.table` "
                f"or `catalog.namespace.schema.table` format, got '{value}'",
            )
        return value
