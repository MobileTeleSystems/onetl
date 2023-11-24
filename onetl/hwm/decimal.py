#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from typing import Any, Callable, Iterator, Optional

from etl_entities.hwm import ColumnIntHWM
from pydantic import StrictInt


class Decimal(StrictInt):
    @classmethod
    def __get_validators__(cls) -> Iterator[Callable]:
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> int:
        if round(float(value)) != float(value):
            raise ValueError(f"{cls.__name__} cannot have fraction part")
        return int(value)


class DecimalHWM(ColumnIntHWM):
    """Same as IntHWM, but allows to pass values like 123.000 (float without fractional part)"""

    value: Optional[Decimal] = None
