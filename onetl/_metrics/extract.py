# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import re
from contextlib import suppress
from datetime import timedelta
from typing import Any

from onetl._metrics.command import SparkCommandMetrics
from onetl._metrics.driver import SparkDriverMetrics
from onetl._metrics.executor import SparkExecutorMetrics
from onetl._metrics.input import SparkInputMetrics
from onetl._metrics.listener.execution import (
    SparkListenerExecution,
    SparkSQLMetricNames,
)
from onetl._metrics.output import SparkOutputMetrics

BYTE_SIZES = {
    "b": 1,
    "kb": 10**3,
    "mb": 10**6,
    "gb": 10**9,
    "tb": 10**12,
    "pb": 10**15,
    "eb": 10**18,
    "kib": 2**10,
    "mib": 2**20,
    "gib": 2**30,
    "tib": 2**40,
    "pib": 2**50,
    "eib": 2**60,
}
BYTE_SIZES.update({k.lower()[0]: v for k, v in BYTE_SIZES.items() if "i" not in k})
byte_string_re = re.compile(r"^\s*(\d*\.?\d+)\s*(\w+)?", re.IGNORECASE)


def _get_int(data: dict[SparkSQLMetricNames, list[str]], key: Any) -> int | None:
    try:
        return int(data[key][0])
    except (KeyError, IndexError, ValueError, TypeError):
        return None


def _get_bytes(data: dict[SparkSQLMetricNames, list[str]], key: Any) -> int | None:
    try:
        raw_value = data[key][0]
    except (KeyError, IndexError):
        return None

    with suppress(ValueError):
        return int(raw_value)

    str_match = byte_string_re.match(raw_value)
    if str_match is None:
        return None

    scalar, unit = str_match.groups()
    if unit is None:
        unit = "b"

    try:
        unit_mult = BYTE_SIZES[unit.lower()]
    except KeyError:
        return None

    return int(float(scalar) * unit_mult)


def extract_metrics_from_execution(execution: SparkListenerExecution) -> SparkCommandMetrics:
    input_read_bytes: int = 0
    input_read_rows: int = 0
    output_bytes: int = 0
    output_rows: int = 0

    run_time_milliseconds: int = 0
    cpu_time_nanoseconds: int = 0
    peak_memory_bytes: int = 0
    memory_spilled_bytes: int = 0
    disk_spilled_bytes: int = 0
    result_size_bytes: int = 0

    # some metrics are per-stage, and have to be summed, others are per-execution
    for job in execution.jobs:
        for stage in job.stages:
            input_read_bytes += stage.metrics.input_metrics.bytes_read
            input_read_rows += stage.metrics.input_metrics.records_read
            output_bytes += stage.metrics.output_metrics.bytes_written
            output_rows += stage.metrics.output_metrics.records_written

            run_time_milliseconds += stage.metrics.executor_run_time_milliseconds
            cpu_time_nanoseconds += stage.metrics.executor_cpu_time_nanoseconds
            peak_memory_bytes += stage.metrics.peak_execution_memory_bytes
            memory_spilled_bytes += stage.metrics.memory_spilled_bytes
            disk_spilled_bytes += stage.metrics.disk_spilled_bytes
            result_size_bytes += stage.metrics.result_size_bytes

    # https://github.com/apache/spark/blob/v3.5.8/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala#L467-L473
    input_file_count = (
        _get_int(execution.metrics, SparkSQLMetricNames.NUMBER_OF_FILES_READ)
        or _get_int(execution.metrics, SparkSQLMetricNames.STATIC_NUMBER_OF_FILES_READ)
        or 0
    )
    input_raw_file_bytes = (
        _get_bytes(execution.metrics, SparkSQLMetricNames.SIZE_OF_FILES_READ)
        or _get_bytes(execution.metrics, SparkSQLMetricNames.STATIC_SIZE_OF_FILES_READ)
        or 0
    )
    input_read_partitions = _get_int(execution.metrics, SparkSQLMetricNames.NUMBER_OF_PARTITIONS_READ) or 0

    output_files = _get_int(execution.metrics, SparkSQLMetricNames.NUMBER_OF_WRITTEN_FILES) or 0
    output_dynamic_partitions = _get_int(execution.metrics, SparkSQLMetricNames.NUMBER_OF_DYNAMIC_PART) or 0

    return SparkCommandMetrics(
        input=SparkInputMetrics(
            read_rows=input_read_rows,
            read_files=input_file_count,
            read_bytes=input_read_bytes,
            raw_file_bytes=input_raw_file_bytes,
            read_partitions=input_read_partitions,
        ),
        output=SparkOutputMetrics(
            written_rows=output_rows,
            written_bytes=output_bytes,
            created_files=output_files,
            created_partitions=output_dynamic_partitions,
        ),
        driver=SparkDriverMetrics(
            in_memory_bytes=result_size_bytes,
        ),
        executor=SparkExecutorMetrics(
            total_run_time=timedelta(milliseconds=run_time_milliseconds),
            total_cpu_time=timedelta(microseconds=cpu_time_nanoseconds / 1000),
            peak_memory_bytes=peak_memory_bytes,
            memory_spilled_bytes=memory_spilled_bytes,
            disk_spilled_bytes=disk_spilled_bytes,
        ),
    )
