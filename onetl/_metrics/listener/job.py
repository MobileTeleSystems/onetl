# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from onetl._metrics.listener.stage import SparkListenerStage, SparkListenerStageStatus
from onetl._util.scala import scala_seq_to_python_list


class SparkListenerJobStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"

    def __str__(self):
        return self.value


@dataclass
class SparkListenerJob:
    id: int
    description: str | None = None
    group_id: str | None = None
    call_site: str | None = None
    status: SparkListenerJobStatus = SparkListenerJobStatus.UNKNOWN

    _stages: dict[int, SparkListenerStage] = field(default_factory=dict, repr=False, init=False)

    @property
    def stages(self) -> list[SparkListenerStage]:
        result = []
        for stage_id in sorted(self._stages.keys()):
            result.append(self._stages[stage_id])
        return result

    @classmethod
    def create(cls, event):
        # https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/scheduler/SparkListenerJobSubmitted.html
        # https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/scheduler/SparkListenerJobCompleted.html
        result = cls(
            id=event.jobId(),
            description=event.properties().get("spark.job.description"),
            group_id=event.properties().get("spark.jobGroup.id"),
            call_site=event.properties().get("callSite.short"),
        )

        stage_ids = scala_seq_to_python_list(event.stageIds())
        stage_infos = scala_seq_to_python_list(event.stageInfos())
        for stage_id, stage_info in zip(stage_ids, stage_infos):
            result._stages[stage_id] = SparkListenerStage.create(stage_info)  # noqa: WPS437

        return result

    def on_job_start(self, event):
        self.status = SparkListenerJobStatus.RUNNING

    def on_job_end(self, event):
        for stage in self._stages.values():
            if stage.status == SparkListenerStageStatus.FAILED:
                self.status = SparkListenerJobStatus.FAILED
                break
        else:
            self.status = SparkListenerJobStatus.SUCCEEDED

    def on_stage_start(self, event):
        stage_id = event.stageInfo().stageId()
        stage = self._stages.get(stage_id)
        if stage:
            stage.on_stage_start(event)

    def on_stage_end(self, event):
        stage_id = event.stageInfo().stageId()
        stage = self._stages.get(stage_id)
        if stage:
            stage.on_stage_end(event)

    # push down events
    def on_task_start(self, event):
        for stage in self._stages.values():
            stage.on_task_start(event)

    def on_task_end(self, event):
        for stage in self._stages.values():
            stage.on_task_end(event)
