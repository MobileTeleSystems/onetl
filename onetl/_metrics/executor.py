# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from datetime import timedelta

from humanize import naturalsize, precisedelta

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl.impl import BaseModel


class SparkExecutorMetrics(BaseModel):
    total_run_time: timedelta = Field(default_factory=timedelta)
    total_cpu_time: timedelta = Field(default_factory=timedelta)
    peak_memory_bytes: int = 0
    memory_spilled_bytes: int = 0
    disk_spilled_bytes: int = 0

    @property
    def is_empty(self) -> bool:
        return not self.total_run_time

    def update(self, other: SparkExecutorMetrics) -> SparkExecutorMetrics:
        self.total_run_time += other.total_run_time
        self.total_cpu_time += other.total_cpu_time
        self.peak_memory_bytes += other.peak_memory_bytes
        self.memory_spilled_bytes += other.memory_spilled_bytes
        self.disk_spilled_bytes += other.disk_spilled_bytes
        return self

    @property
    def details(self) -> str:
        if self.is_empty:
            return "No data"

        result = [
            f"Total run time: {precisedelta(self.total_run_time)}",
            f"Total CPU time: {precisedelta(self.total_cpu_time)}",
        ]

        if self.peak_memory_bytes:
            result.append(f"Peak memory: {naturalsize(self.peak_memory_bytes)}")

        if self.memory_spilled_bytes:
            result.append(f"Memory spilled: {naturalsize(self.memory_spilled_bytes)}")

        if self.disk_spilled_bytes:
            result.append(f"Disk spilled: {naturalsize(self.disk_spilled_bytes)}")

        return os.linesep.join(result)

    def __str__(self):
        return self.details
