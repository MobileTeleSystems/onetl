# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from onetl._metrics.listener.task import SparkListenerTask, SparkListenerTaskMetrics


class SparkListenerStageStatus(str, Enum):
    ACTIVE = "ACTIVE"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    PENDING = "PENDING"
    SKIPPED = "SKIPPED"

    def __str__(self):
        return self.value


@dataclass
class SparkListenerStage:
    # https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/scheduler/StageInfo.html
    id: int
    status: SparkListenerStageStatus = SparkListenerStageStatus.PENDING
    metrics: SparkListenerTaskMetrics = field(default_factory=SparkListenerTaskMetrics, repr=False, init=False)
    _tasks: dict[int, SparkListenerTask] = field(default_factory=dict, repr=False, init=False)

    @property
    def tasks(self) -> list[SparkListenerTask]:
        result = []
        for task_id in sorted(self._tasks.keys()):
            result.append(self._tasks[task_id])
        return result

    @classmethod
    def create(cls, stage_info):
        return cls(id=stage_info.stageId())

    def on_stage_start(self, event):
        # https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/scheduler/SparkListenerStageSubmitted.html
        self.status = SparkListenerStageStatus.ACTIVE

    def on_stage_end(self, event):
        # https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/scheduler/SparkListenerStageCompleted.html
        stage_info = event.stageInfo()
        if stage_info.failureReason().isDefined():
            self.status = SparkListenerStageStatus.FAILED
        elif not self.tasks:
            self.status = SparkListenerStageStatus.SKIPPED
        else:
            self.status = SparkListenerStageStatus.COMPLETE

        self.metrics = SparkListenerTaskMetrics.create(stage_info.taskMetrics())

    def on_task_start(self, event):
        task_info = event.taskInfo()
        task_id = task_info.taskId()
        self._tasks[task_id] = SparkListenerTask.create(task_info)

    def on_task_end(self, event):
        task_id = event.taskInfo().taskId()
        task = self._tasks.get(task_id)
        if task:
            task.on_task_end(event)
