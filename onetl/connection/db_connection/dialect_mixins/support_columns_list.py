# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings


class SupportColumns:
    def validate_columns(
        self,
        columns: str | list[str] | None,
    ) -> list[str] | None:
        if columns is None:
            return ["*"]

        if isinstance(columns, str):
            warnings.warn(
                f"Passing DBReader(columns={columns!r}) is deprecated since v0.10.0 and will be removed in v1.0.0. "
                f"Use DBReader(columns=[{columns!r}] instead",
                category=UserWarning,
                stacklevel=3,
            )
            return [columns]

        return list(columns)
