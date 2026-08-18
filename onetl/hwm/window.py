# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Edge:
    value: Any = None
    including: bool = True

    def is_set(self) -> bool:
        return self.value is not None


@dataclass(slots=True)
class Window:
    expression: str
    start_from: Edge = field(default_factory=Edge)
    stop_at: Edge = field(default_factory=Edge)
