# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations


class SupportNameWithSchemaOnly:
    def validate_name(self, value: str) -> str:
        if value.count(".") != 1:
            raise ValueError(
                f"Name should be passed in `schema.name` format, got '{value}'",
            )

        return value
