# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Edge:
    value: Any = None
    including: bool = True

    def is_set(self) -> bool:
        return self.value is not None


@dataclass
class Window:
    expression: str
    start_from: Edge = field(default_factory=Edge)
    stop_at: Edge = field(default_factory=Edge)
