# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

from onetl._metrics.command import SparkCommandMetrics
from onetl._metrics.extract import extract_metrics_from_execution
from onetl._metrics.listener import SparkMetricsListener

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class SparkMetricsRecorder:
    def __init__(self, spark: SparkSession):
        self._listener = SparkMetricsListener(spark=spark)

    def __enter__(self):
        self._listener.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._listener.__exit__(exc_type, exc_val, exc_tb)

    def metrics(self) -> SparkCommandMetrics:
        result = SparkCommandMetrics()
        for execution in self._listener.executions:
            result = result.update(extract_metrics_from_execution(execution))
        return result
