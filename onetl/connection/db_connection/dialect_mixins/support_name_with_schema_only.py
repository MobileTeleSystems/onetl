# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0


class SupportNameWithSchemaOnly:
    def validate_name(self, value: str) -> str:
        if "." not in value:
            msg = f"Name should be passed in `schema.name` format, got '{value}'"
            raise ValueError(msg)

        return value
