from typing import Any

from etl_entities.hwm import HWM


class AutoHWM(HWM):
    entity: str
    expression: Any = None

    def covers(self, value):
        pass

    def update(self, value):
        pass
