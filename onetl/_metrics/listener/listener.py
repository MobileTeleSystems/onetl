# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field
from threading import current_thread
from typing import ClassVar

from onetl._metrics.listener.base import BaseSparkListener
from onetl._metrics.listener.execution import (
    SparkListenerExecution,
    SparkSQLMetricNames,
)

KNOWN_METRICS = SparkSQLMetricNames.values()


@dataclass
class SparkMetricsListener(BaseSparkListener):
    THREAD_ID_KEY = "python.thread.id"
    SQL_START_CLASS_NAME: ClassVar[str] = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
    SQL_STOP_CLASS_NAME: ClassVar[str] = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"

    _thread_id: str = field(default_factory=lambda: str(current_thread().ident), repr=False, init=False)
    _recorded_executions: dict[int, SparkListenerExecution] = field(default_factory=dict, repr=False, init=False)

    def activate(self):
        # we cannot override execution_id property as it set by Spark
        # we also cannot use job tags, as they were implemented only in Spark 3.5+
        self.spark.sparkContext.setLocalProperty(self.THREAD_ID_KEY, self._thread_id)
        return super().activate()

    def reset(self):
        self._recorded_executions.clear()
        return self

    @property
    def executions(self):
        return [
            execution for execution in self._recorded_executions.values() if execution.external_id == self._thread_id
        ]

    def __enter__(self):
        """Record only executions performed by current Spark thread.

        It is important to use this method only in combination with
        :obj:`pyspark.util.InheritableThread` to preserve thread-local variables
        between Python thread and Java thread.
        """
        self.reset()
        return super().__enter__()

    def onOtherEvent(self, event):
        class_name = event.getClass().getName()
        if class_name == self.SQL_START_CLASS_NAME:
            self.onExecutionStart(event)
        elif class_name == self.SQL_STOP_CLASS_NAME:
            self.onExecutionEnd(event)

    def onExecutionStart(self, event):
        execution_id = event.executionId()
        description = event.description()
        execution = SparkListenerExecution(
            id=execution_id,
            description=description,
        )
        self._recorded_executions[execution_id] = execution
        execution.on_execution_start(event)

    def onExecutionEnd(self, event):
        execution_id = event.executionId()
        execution = self._recorded_executions.get(execution_id)
        if execution:
            execution.on_execution_end(event)

            # Get execution metrics from SQLAppStatusStore,
            # as SparkListenerSQLExecutionEnd event does not provide them:
            # https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala
            session_status_store = self.spark._jsparkSession.sharedState().statusStore()  # noqa: WPS437
            raw_execution = session_status_store.execution(execution.id).get()
            metrics = raw_execution.metrics()
            metric_values = session_status_store.executionMetrics(execution.id)
            for i in range(metrics.size()):
                metric = metrics.apply(i)
                metric_name = metric.name()
                if metric_name not in KNOWN_METRICS:
                    continue
                metric_value = metric_values.get(metric.accumulatorId())
                if not metric_value.isDefined():
                    continue
                execution.metrics[SparkSQLMetricNames(metric_name)].append(metric_value.get())

    def onJobStart(self, event):
        execution_id = event.properties().get("spark.sql.execution.id")
        execution_thread_id = event.properties().get(self.THREAD_ID_KEY)
        if execution_id is None:
            # single job execution
            job_id = event.jobId()
            execution = SparkListenerExecution(
                id=job_id,
                description=event.properties().get("spark.job.description"),
                external_id=execution_thread_id,
            )
            self._recorded_executions[job_id] = execution
        else:
            execution = self._recorded_executions.get(int(execution_id))
            if execution is None:
                return

            if execution_thread_id:
                # SparkListenerSQLExecutionStart does not have properties, but SparkListenerJobStart does,
                # use it as a source of external_id
                execution.external_id = execution_thread_id

        execution.on_job_start(event)

    def onJobEnd(self, event):
        for execution in self._recorded_executions.values():
            execution.on_job_end(event)

    def onStageSubmitted(self, event):
        for execution in self._recorded_executions.values():
            execution.on_stage_start(event)

    def onStageCompleted(self, event):
        for execution in self._recorded_executions.values():
            execution.on_stage_end(event)

    def onTaskStart(self, event):
        for execution in self._recorded_executions.values():
            execution.on_task_start(event)

    def onTaskEnd(self, event):
        for execution in self._recorded_executions.values():
            execution.on_task_end(event)
