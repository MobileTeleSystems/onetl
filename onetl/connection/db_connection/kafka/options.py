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


from onetl.impl import GenericOptions

PROHIBITED_OPTIONS = frozenset(
    (
        "assign",
        "endingOffsets",
        "endingOffsetsByTimestamp",
        "kafka.*",
        "startingOffsets",
        "startingOffsetsByTimestamp",
        "startingOffsetsByTimestampStrategy",
        "startingTimestamp",
        "subscribe",
        "subscribePattern",
        "topic",
    ),
)

KNOWN_READ_OPTIONS = frozenset(
    (
        "endingTimestamp",
        "failOnDataLoss",
        "fetchOffset.numRetries",
        "fetchOffset.retryIntervalMs",
        "groupIdPrefix",
        "includeHeaders",
        "kafkaConsumer.pollTimeoutMs",
        "maxOffsetsPerTrigger",
        "maxTriggerDelay",
        "minOffsetsPerTrigger",
        "minPartitions",
    ),
)

KNOWN_WRITE_OPTIONS = frozenset(
    ("includeHeaders",),
)


class KafkaReadOptions(GenericOptions):
    """Reading options for Kafka connector.

    .. note ::

        You can pass any value
        `supported by connector <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_,
        even if it is not mentioned in this documentation.

        The set of supported options depends on connector version.

    .. warning::

        Options:
            * ``assign``
            * ``endingOffsets``
            * ``endingOffsetsByTimestamp``
            * ``kafka.*``
            * ``startingOffsets``
            * ``startingOffsetsByTimestamp``
            * ``startingOffsetsByTimestampStrategy``
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

        Kafka.ReadOptions(
            maxOffsetsPerTrigger=10000,
        )
    """

    class Config:
        prohibited_options = PROHIBITED_OPTIONS
        known_options = KNOWN_READ_OPTIONS
        extra = "allow"


class KafkaWriteOptions(GenericOptions):
    """Writing options for Kafka connector.

    .. note ::

        You can pass any value
        `supported by connector <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_,
        even if it is not mentioned in this documentation.

        The set of supported options depends on connector version.

    .. warning::

        Options:
            * ``assign``
            * ``endingOffsets``
            * ``endingOffsetsByTimestamp``
            * ``kafka.*``
            * ``startingOffsets``
            * ``startingOffsetsByTimestamp``
            * ``startingOffsetsByTimestampStrategy``
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
            includeHeaders=False,
        )
    """

    class Config:
        prohibited_options = PROHIBITED_OPTIONS
        known_options = KNOWN_WRITE_OPTIONS
        extra = "allow"