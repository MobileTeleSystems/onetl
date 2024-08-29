# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import textwrap

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl._metrics.driver import SparkDriverMetrics
from onetl._metrics.executor import SparkExecutorMetrics
from onetl._metrics.input import SparkInputMetrics
from onetl._metrics.output import SparkOutputMetrics
from onetl.impl import BaseModel

INDENT = " " * 4


class SparkCommandMetrics(BaseModel):
    input: SparkInputMetrics = Field(default_factory=SparkInputMetrics)
    output: SparkOutputMetrics = Field(default_factory=SparkOutputMetrics)
    driver: SparkDriverMetrics = Field(default_factory=SparkDriverMetrics)
    executor: SparkExecutorMetrics = Field(default_factory=SparkExecutorMetrics)

    @property
    def is_empty(self) -> bool:
        return all([self.input.is_empty, self.output.is_empty])

    def update(self, other: SparkCommandMetrics) -> SparkCommandMetrics:
        self.input.update(other.input)
        self.output.update(other.output)
        self.driver.update(other.driver)
        self.executor.update(other.executor)
        return self

    @property
    def details(self) -> str:
        if self.is_empty:
            return "No data"

        result = []
        if not self.input.is_empty:
            result.append(f"Input:{os.linesep}{textwrap.indent(self.input.details, INDENT)}")
        if not self.output.is_empty:
            result.append(f"Output:{os.linesep}{textwrap.indent(self.output.details, INDENT)}")
        if not self.driver.is_empty:
            result.append(f"Driver:{os.linesep}{textwrap.indent(self.driver.details, INDENT)}")
        if not self.executor.is_empty:
            result.append(f"Executor:{os.linesep}{textwrap.indent(self.executor.details, INDENT)}")

        return os.linesep.join(result)

    def __str__(self):
        return self.details
