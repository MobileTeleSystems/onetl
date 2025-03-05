# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum

from onetl._metrics.listener.job import SparkListenerJob, SparkListenerJobStatus


class SparkListenerExecutionStatus(str, Enum):
    STARTED = "STARTED"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"

    def __str__(self):
        return self.value


class SparkSQLMetricNames(str, Enum):  # noqa: WPS338
    # Metric names passed to SQLMetrics.createMetric(...)
    # But only those we're interested in.

    # https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala#L231
    NUMBER_OF_PARTITIONS_READ = "number of partitions read"

    # https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala#L225-L227
    NUMBER_OF_FILES_READ = "number of files read"
    SIZE_OF_FILES_READ = "size of files read"

    # https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala#L225-L227
    STATIC_NUMBER_OF_FILES_READ = "static number of files read"
    STATIC_SIZE_OF_FILES_READ = "static size of files read"

    # https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/BasicWriteStatsTracker.scala#L241-L246
    NUMBER_OF_DYNAMIC_PART = "number of dynamic part"
    NUMBER_OF_WRITTEN_FILES = "number of written files"

    def __str__(self):
        return self.value

    @classmethod
    def values(cls):
        return set(cls.__members__.values())


@dataclass
class SparkListenerExecution:
    id: int
    description: str | None = None
    external_id: str | None = None
    status: SparkListenerExecutionStatus = SparkListenerExecutionStatus.STARTED

    # These metrics are emitted by any command performed within this execution, so we can have multiple values.
    # Some metrics can be summarized, but some not, so we store a list.
    metrics: dict[SparkSQLMetricNames, list[str]] = field(default_factory=lambda: defaultdict(list), repr=False)

    _jobs: dict[int, SparkListenerJob] = field(default_factory=dict, repr=False, init=False)

    @property
    def jobs(self) -> list[SparkListenerJob]:
        result = []
        for job_id in sorted(self._jobs.keys()):
            result.append(self._jobs[job_id])
        return result

    def on_execution_start(self, event):
        # https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLListener.scala#L44-L58
        self.status = SparkListenerExecutionStatus.STARTED

    def on_execution_end(self, event):
        # https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLListener.scala#L61-L83
        for job in self._jobs.values():
            if job.status == SparkListenerJobStatus.FAILED:
                self.status = SparkListenerExecutionStatus.FAILED
                break
        else:
            self.status = SparkListenerExecutionStatus.COMPLETE

    def on_job_start(self, event):
        job_id = event.jobId()
        job = SparkListenerJob.create(event)
        self._jobs[job_id] = job
        job.on_job_start(event)

    def on_job_end(self, event):
        job_id = event.jobId()
        job = self._jobs.get(job_id)

        if job:
            job.on_job_end(event)

        # in some cases Execution consists of just one job with same id
        if job_id == self.id:
            self.on_execution_end(event)

    # push down events
    def on_stage_start(self, event):
        for job in self._jobs.values():
            job.on_stage_start(event)

    def on_stage_end(self, event):
        for job in self._jobs.values():
            job.on_stage_end(event)

    def on_task_start(self, event):
        for job in self._jobs.values():
            job.on_task_start(event)

    def on_task_end(self, event):
        for job in self._jobs.values():
            job.on_task_end(event)
