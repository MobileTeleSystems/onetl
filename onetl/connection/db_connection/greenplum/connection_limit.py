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

from __future__ import annotations

import textwrap
from dataclasses import dataclass


@dataclass
class GreenplumConnectionLimit:
    maximum: int
    reserved: int
    occupied: int

    @property
    def available(self) -> int:
        return self.maximum - self.reserved - self.occupied

    @property
    def summary(self) -> str:
        return textwrap.dedent(
            f"""
            available connections: {self.available}
            occupied: {self.occupied}
            max: {self.maximum} ("max_connection" in postgresql.conf)
            reserved: {self.reserved} ("superuser_reserved_connections" in postgresql.conf)
            """,
        ).strip()
