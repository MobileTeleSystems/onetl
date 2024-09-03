# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from pprint import pformat

from humanize import naturalsize

from onetl.impl import BaseModel


class SparkOutputMetrics(BaseModel):
    written_bytes: int = 0
    written_rows: int = 0
    created_files: int = 0
    created_partitions: int = 0

    @property
    def is_empty(self) -> bool:
        return not any([self.written_bytes, self.written_rows, self.created_files])

    def update(self, other: SparkOutputMetrics) -> SparkOutputMetrics:
        self.written_bytes += other.written_bytes
        self.written_rows += other.written_rows
        self.created_files += other.created_files
        self.created_partitions = max([self.created_partitions, other.created_partitions])
        return self

    @property
    def details(self) -> str:
        if self.is_empty:
            return "No data"

        result = []
        result.append(f"Written rows: {pformat(self.written_rows)}")

        if self.written_bytes:
            result.append(f"Written size: {naturalsize(self.written_bytes)}")

        if self.created_files:
            result.append(f"Created files: {pformat(self.created_files)}")

        if self.created_partitions:
            result.append(f"Created partitions: {pformat(self.created_partitions)}")

        return os.linesep.join(result)

    def __str__(self):
        return self.details
