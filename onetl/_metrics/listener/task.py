# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class SparkListenerTaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    KILLED = "KILLED"

    def __str__(self):
        return self.value


@dataclass
class SparkListenerTaskInputMetrics:
    bytes_read: int = 0
    records_read: int = 0

    @classmethod
    def create(cls, task_input_metrics):
        return cls(
            bytes_read=task_input_metrics.bytesRead(),
            records_read=task_input_metrics.recordsRead(),
        )


@dataclass
class SparkListenerTaskOutputMetrics:
    bytes_written: int = 0
    records_written: int = 0

    @classmethod
    def create(cls, task_output_metrics):
        return cls(
            bytes_written=task_output_metrics.bytesWritten(),
            records_written=task_output_metrics.recordsWritten(),
        )


@dataclass
class SparkListenerTaskMetrics:
    """Python representation of Spark TaskMetrics object.

    See `documentation <https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics>`_.
    """

    executor_run_time_milliseconds: int = 0
    executor_cpu_time_nanoseconds: int = 0
    peak_execution_memory_bytes: int = 0
    memory_spilled_bytes: int = 0
    disk_spilled_bytes: int = 0
    result_size_bytes: int = 0
    input_metrics: SparkListenerTaskInputMetrics = field(default_factory=SparkListenerTaskInputMetrics)
    output_metrics: SparkListenerTaskOutputMetrics = field(default_factory=SparkListenerTaskOutputMetrics)

    @classmethod
    def create(cls, task_metrics):
        if not task_metrics:
            return cls()
        return cls(
            executor_run_time_milliseconds=task_metrics.executorRunTime(),
            executor_cpu_time_nanoseconds=task_metrics.executorCpuTime(),
            peak_execution_memory_bytes=task_metrics.peakExecutionMemory(),
            memory_spilled_bytes=task_metrics.memoryBytesSpilled(),
            disk_spilled_bytes=task_metrics.diskBytesSpilled(),
            result_size_bytes=task_metrics.resultSize(),
            input_metrics=SparkListenerTaskInputMetrics.create(task_metrics.inputMetrics()),
            output_metrics=SparkListenerTaskOutputMetrics.create(task_metrics.outputMetrics()),
        )


@dataclass
class SparkListenerTask:
    id: int
    status: SparkListenerTaskStatus = SparkListenerTaskStatus.PENDING
    metrics: SparkListenerTaskMetrics | None = field(default=None, repr=False, init=False)

    @classmethod
    def create(cls, task_info):
        # https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/scheduler/TaskInfo.html
        return cls(id=task_info.taskId())

    def on_task_start(self, event):
        # https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/scheduler/SparkListenerTaskStart.html
        self.status = SparkListenerTaskStatus(event.taskInfo().status())

    def on_task_end(self, event):
        # https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/scheduler/SparkListenerTaskEnd.html
        self.status = SparkListenerTaskStatus(event.taskInfo().status())
        self.metrics = SparkListenerTaskMetrics.create(event.taskMetrics())
