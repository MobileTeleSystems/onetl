from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class Statement:
    expression: Callable | str
    operator: Any
    value: Any
