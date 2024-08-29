# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os

from humanize import naturalsize

from onetl.impl import BaseModel

# Metrics themselves are considered a part of driver result,
# ignore if result is smaller than 1MB
MIN_DRIVER_BYTES = 1_000_000


class SparkDriverMetrics(BaseModel):
    in_memory_bytes: int = 0

    @property
    def is_empty(self) -> bool:
        return self.in_memory_bytes < MIN_DRIVER_BYTES

    def update(self, other: SparkDriverMetrics) -> SparkDriverMetrics:
        self.in_memory_bytes += other.in_memory_bytes
        return self

    @property
    def details(self) -> str:
        if self.is_empty:
            return "No data"

        result = []
        if self.in_memory_bytes >= MIN_DRIVER_BYTES:
            result.append(f"In-memory data (approximate): {naturalsize(self.in_memory_bytes)}")

        return os.linesep.join(result)

    def __str__(self):
        return self.details
