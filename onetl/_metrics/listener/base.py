# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from onetl._util.java import get_java_gateway, start_callback_server

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class BaseSparkListener:
    """Base no-op SparkListener implementation.

    See `SparkListener <https://spark.apache.org/docs/3.5.6/api/java/org/apache/spark/scheduler/SparkListener.html>`_ interface.
    """

    spark: SparkSession

    def activate(self):
        start_callback_server(self.spark)

        # passing python listener object directly to addSparkListener or removeSparkListener leads to creating new java object each time.
        # But removeSparkListener call has effect only on the same Java object passed to removeSparkListener.
        # So we need to explicitly create Java object, and then pass it both calls.
        gateway = get_java_gateway(self.spark)
        java_list = gateway.jvm.java.util.ArrayList()
        java_list.append(self)
        self._java_listener = java_list[0]

        spark_context = self.spark.sparkContext._jsc.sc()  # noqa: WPS437
        spark_context.addSparkListener(self._java_listener)

    def deactivate(self):
        with suppress(Exception):
            spark_context = self.spark.sparkContext._jsc.sc()  # noqa: WPS437
            spark_context.removeSparkListener(self._java_listener)

        with suppress(Exception):
            del self._java_listener

    def __enter__(self):
        self.activate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.deactivate()

    def __del__(self):  # noqa: WPS603
        # If current object is collected by GC, deactivate listener
        # and free bind Java object
        self.deactivate()

    def equals(self, other):
        # Java does not provide proper way to get object id for comparison,
        # so we compare string representation which should contain some form of id
        return other.toString() == self._java_listener.toString()

    def toString(self):
        return type(self).__qualname__ + "@" + hex(id(self))

    def hashCode(self):
        return hash(self)

    # no cover: start
    # method names are important for Java interface compatibility!
    def onApplicationEnd(self, application):
        pass

    def onApplicationStart(self, application):
        pass

    def onBlockManagerAdded(self, block_manager):
        pass

    def onBlockManagerRemoved(self, block_manager):
        pass

    def onBlockUpdated(self, block):
        pass

    def onEnvironmentUpdate(self, environment):
        pass

    def onExecutorAdded(self, executor):
        pass

    def onExecutorMetricsUpdate(self, executor):
        pass

    def onExecutorRemoved(self, executor):
        pass

    def onExecutorBlacklisted(self, event):
        pass

    def onExecutorBlacklistedForStage(self, event):
        pass

    def onExecutorExcluded(self, event):
        pass

    def onExecutorExcludedForStage(self, event):
        pass

    def onExecutorUnblacklisted(self, event):
        pass

    def onExecutorUnexcluded(self, event):
        pass

    def onJobStart(self, event):
        pass

    def onJobEnd(self, event):
        pass

    def onNodeBlacklisted(self, node):
        pass

    def onNodeBlacklistedForStage(self, stage):
        pass

    def onNodeExcluded(self, node):
        pass

    def onNodeExcludedForStage(self, node):
        pass

    def onNodeUnblacklisted(self, node):
        pass

    def onNodeUnexcluded(self, node):
        pass

    def onOtherEvent(self, event):
        pass

    def onResourceProfileAdded(self, resource_profile):
        pass

    def onSpeculativeTaskSubmitted(self, task):
        pass

    def onStageCompleted(self, event):
        pass

    def onStageExecutorMetrics(self, metrics):
        pass

    def onStageSubmitted(self, event):
        pass

    def onTaskEnd(self, event):
        pass

    def onTaskGettingResult(self, task):
        pass

    def onTaskStart(self, event):
        pass

    def onUnpersistRDD(self, rdd):
        pass

    def onUnschedulableTaskSetAdded(self, task_set):
        pass

    def onUnschedulableTaskSetRemoved(self, task_set):
        pass

    # no cover: stop
    class Java:
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]
