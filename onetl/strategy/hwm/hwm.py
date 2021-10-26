from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from onetl.strategy.hwm.hwm_class_registry import register_hwm_class


class HWM:
    value: Any


@dataclass
class ColumnHWM(HWM):
    table: str
    column: str
    value: Any = None

    def __str__(self):
        return f"{self.table}.{self.column}"


@register_hwm_class("int", "integer")
@dataclass
class IntHWM(ColumnHWM):
    value: int | None = None


@register_hwm_class("date")
@dataclass
class DateHWM(ColumnHWM):
    value: date | None = None


@register_hwm_class("datetime", "timestamp")
@dataclass
class DateTimeHWM(ColumnHWM):
    value: datetime | None = None


@register_hwm_class("float", "long")
@dataclass
class FloatHWM(ColumnHWM):
    value: float | None = None

    def __post_init__(self):
        if self.value is not None:
            self.value = float(self.value)  # noqa: WPS601


@register_hwm_class("files")
@dataclass
class FileListHWM(HWM):
    path: Path
    value: list[Path] = field(default_factory=list)

    def __str__(self):
        return f"{self.path}"
