# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
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
