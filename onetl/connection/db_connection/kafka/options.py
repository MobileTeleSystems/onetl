#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from enum import Enum

from pydantic import Field, root_validator

from onetl.impl import GenericOptions

PROHIBITED_OPTIONS = frozenset(
    (
        "assign",
        "endingOffsets",
        "endingOffsetsByTimestamp",
        "endingTimestamp",
        "kafka.*",
        "startingOffsets",
        "startingOffsetsByTimestamp",
        "startingTimestamp",
        "subscribe",
        "subscribePattern",
        "topic",
    ),
)

KNOWN_READ_WRITE_OPTIONS = frozenset(
    (
        # not adding this to class itself because headers support was added to Spark only in 3.0
        # https://issues.apache.org/jira/browse/SPARK-23539
        "includeHeaders",
    ),
)

KNOWN_READ_OPTIONS = frozenset(
    (
        "failOnDataLoss",
        "fetchOffset.numRetries",
        "fetchOffset.retryIntervalMs",
        "groupIdPrefix",
        "kafkaConsumer.pollTimeoutMs",
        "maxOffsetsPerTrigger",
        "maxTriggerDelay",
        "minOffsetsPerTrigger",
        "minPartitions",
    ),
)


class KafkaTopicExistBehaviorKafka(str, Enum):
    ERROR = "error"
    APPEND = "append"

    def __str__(self) -> str:
        return str(self.value)


class KafkaReadOptions(GenericOptions):
    """Reading options for Kafka connector.

    .. note ::

        You can pass any value
        `supported by connector <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_,
        even if it is not mentioned in this documentation.

        The set of supported options depends on connector version. See link above.

    .. warning::

        Options:
            * ``assign``
            * ``endingOffsets``
            * ``endingOffsetsByTimestamp``
            * ``kafka.*``
            * ``startingOffsets``
            * ``startingOffsetsByTimestamp``
            * ``startingTimestamp``
            * ``subscribe``
            * ``subscribePattern``
            * ``topic``

        populated from connection attributes, and cannot be set in ``KafkaReadOptions`` class and be overridden
        by the user to avoid issues.

    Examples
    --------

    Read options initialization

    .. code:: python

        options = Kafka.ReadOptions(
            minPartitions=50,
            includeHeaders=True,
        )
    """

    class Config:
        prohibited_options = PROHIBITED_OPTIONS
        known_options = KNOWN_READ_OPTIONS | KNOWN_READ_WRITE_OPTIONS
        extra = "allow"


class KafkaWriteOptions(GenericOptions):
    """Writing options for Kafka connector.

    .. note ::

        You can pass any value
        `supported by connector <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_,
        even if it is not mentioned in this documentation.

        The set of supported options depends on connector version. See link above.

    .. warning::

        Options:
            * ``assign``
            * ``endingOffsets``
            * ``endingOffsetsByTimestamp``
            * ``kafka.*``
            * ``startingOffsets``
            * ``startingOffsetsByTimestamp``
            * ``startingTimestamp``
            * ``subscribe``
            * ``subscribePattern``
            * ``topic``

        populated from connection attributes, and cannot be set in ``KafkaWriteOptions`` class and be overridden
        by the user to avoid issues.

    Examples
    --------

    Write options initialization

    .. code:: python

        options = Kafka.WriteOptions(
            if_exists="append",
            includeHeaders=False,
        )
    """

    if_exists: KafkaTopicExistBehaviorKafka = Field(default=KafkaTopicExistBehaviorKafka.APPEND)
    """Behavior of writing data into existing topic.

    Same as ``df.write.mode(...)``.

    Possible values:
        * ``append`` (default) - Adds new objects into existing topic.
        * ``error`` - Raises an error if topic already exists.
    """

    class Config:
        prohibited_options = PROHIBITED_OPTIONS | KNOWN_READ_OPTIONS
        known_options = KNOWN_READ_WRITE_OPTIONS
        extra = "allow"

    @root_validator(pre=True)
    def _mode_is_restricted(cls, values):
        if "mode" in values:
            raise ValueError("Parameter `mode` is not allowed. Please use `if_exists` parameter instead.")
        return values
