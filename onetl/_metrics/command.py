# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
import textwrap
from dataclasses import dataclass, field

from onetl._metrics.driver import SparkDriverMetrics
from onetl._metrics.executor import SparkExecutorMetrics
from onetl._metrics.input import SparkInputMetrics
from onetl._metrics.output import SparkOutputMetrics

INDENT = " " * 4


@dataclass(slots=True)
class SparkCommandMetrics:
    input: SparkInputMetrics = field(default_factory=SparkInputMetrics)
    output: SparkOutputMetrics = field(default_factory=SparkOutputMetrics)
    driver: SparkDriverMetrics = field(default_factory=SparkDriverMetrics)
    executor: SparkExecutorMetrics = field(default_factory=SparkExecutorMetrics)

    @property
    def is_empty(self) -> bool:
        return all([self.input.is_empty, self.output.is_empty])

    def update(self, other: "SparkCommandMetrics") -> "SparkCommandMetrics":
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
