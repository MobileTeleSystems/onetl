# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from pprint import pformat

from humanize import naturalsize

from onetl.impl import BaseModel


class SparkInputMetrics(BaseModel):
    read_rows: int = 0
    read_files: int = 0
    read_partitions: int = 0
    read_bytes: int = 0
    raw_file_bytes: int = 0

    @property
    def is_empty(self) -> bool:
        return not any([self.read_bytes, self.read_files, self.read_rows])

    def update(self, other: SparkInputMetrics) -> SparkInputMetrics:
        self.read_rows += other.read_rows
        self.read_files += other.read_files
        self.read_partitions += other.read_partitions
        self.read_bytes += other.read_bytes
        self.raw_file_bytes += other.raw_file_bytes
        return self

    @property
    def details(self) -> str:
        if self.is_empty:
            return "No data"

        result = []
        result.append(f"Read rows: {pformat(self.read_rows)}")

        if self.read_partitions:
            result.append(f"Read partitions: {pformat(self.read_partitions)}")

        if self.read_files:
            result.append(f"Read files: {pformat(self.read_files)}")

        if self.read_bytes:
            result.append(f"Read size: {naturalsize(self.read_bytes)}")

        if self.raw_file_bytes and self.read_bytes != self.raw_file_bytes:
            result.append(f"Raw files size: {naturalsize(self.raw_file_bytes)}")

        return os.linesep.join(result)

    def __str__(self):
        return self.details
