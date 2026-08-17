# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from onetl._util.java import get_java_gateway, start_callback_server

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class BaseSparkListener:
    """Base no-op SparkListener implementation.

    See [SparkListener](https://spark.apache.org/docs/3.5.8/api/java/org/apache/spark/scheduler/SparkListener.html) interface.
    """

    spark: "SparkSession"

    def activate(self):
        start_callback_server(self.spark)

        # passing python listener object directly to addSparkListener or removeSparkListener
        # leads to creating new java object each time.
        # But removeSparkListener call has effect only on the same Java object passed to removeSparkListener.
        # So we need to explicitly create Java object, and then pass it both calls.
        gateway = get_java_gateway(self.spark)
        java_list = gateway.jvm.java.util.ArrayList()
        java_list.append(self)
        self._java_listener = java_list[0]

        spark_context = self.spark.sparkContext._jsc.sc()  # noqa: SLF001
        spark_context.addSparkListener(self._java_listener)

    def deactivate(self):
        with suppress(Exception):
            spark_context = self.spark.sparkContext._jsc.sc()  # noqa: SLF001
            spark_context.removeSparkListener(self._java_listener)

        with suppress(Exception):
            del self._java_listener

    def __enter__(self):
        self.activate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.deactivate()

    def __del__(self):
        # If current object is collected by GC, deactivate listener
        # and free bind Java object
        self.deactivate()

    def equals(self, other):
        # Java does not provide proper way to get object id for comparison,
        # so we compare string representation which should contain some form of id
        return other.toString() == self._java_listener.toString()

    def toString(self):  # noqa: N802
        return type(self).__qualname__ + "@" + hex(id(self))

    def hashCode(self):  # noqa: N802
        return hash(self)

    # no cover: start
    # method names are important for Java interface compatibility!
    def onApplicationEnd(self, application):  # noqa: N802
        pass

    def onApplicationStart(self, application):  # noqa: N802
        pass

    def onBlockManagerAdded(self, block_manager):  # noqa: N802
        pass

    def onBlockManagerRemoved(self, block_manager):  # noqa: N802
        pass

    def onBlockUpdated(self, block):  # noqa: N802
        pass

    def onEnvironmentUpdate(self, environment):  # noqa: N802
        pass

    def onExecutorAdded(self, executor):  # noqa: N802
        pass

    def onExecutorMetricsUpdate(self, executor):  # noqa: N802
        pass

    def onExecutorRemoved(self, executor):  # noqa: N802
        pass

    def onExecutorBlacklisted(self, event):  # noqa: N802
        pass

    def onExecutorBlacklistedForStage(self, event):  # noqa: N802
        pass

    def onExecutorExcluded(self, event):  # noqa: N802
        pass

    def onExecutorExcludedForStage(self, event):  # noqa: N802
        pass

    def onExecutorUnblacklisted(self, event):  # noqa: N802
        pass

    def onExecutorUnexcluded(self, event):  # noqa: N802
        pass

    def onJobStart(self, event):  # noqa: N802
        pass

    def onJobEnd(self, event):  # noqa: N802
        pass

    def onNodeBlacklisted(self, node):  # noqa: N802
        pass

    def onNodeBlacklistedForStage(self, stage):  # noqa: N802
        pass

    def onNodeExcluded(self, node):  # noqa: N802
        pass

    def onNodeExcludedForStage(self, node):  # noqa: N802
        pass

    def onNodeUnblacklisted(self, node):  # noqa: N802
        pass

    def onNodeUnexcluded(self, node):  # noqa: N802
        pass

    def onOtherEvent(self, event):  # noqa: N802
        pass

    def onResourceProfileAdded(self, resource_profile):  # noqa: N802
        pass

    def onSpeculativeTaskSubmitted(self, task):  # noqa: N802
        pass

    def onStageCompleted(self, event):  # noqa: N802
        pass

    def onStageExecutorMetrics(self, metrics):  # noqa: N802
        pass

    def onStageSubmitted(self, event):  # noqa: N802
        pass

    def onTaskEnd(self, event):  # noqa: N802
        pass

    def onTaskGettingResult(self, task):  # noqa: N802
        pass

    def onTaskStart(self, event):  # noqa: N802
        pass

    def onUnpersistRDD(self, rdd):  # noqa: N802
        pass

    def onUnschedulableTaskSetAdded(self, task_set):  # noqa: N802
        pass

    def onUnschedulableTaskSetRemoved(self, task_set):  # noqa: N802
        pass

    # no cover: stop
    class Java:
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]  # noqa: RUF012
