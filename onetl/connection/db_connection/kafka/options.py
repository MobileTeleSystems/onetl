# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from enum import Enum

try:
    from pydantic.v1 import Field, root_validator
except (ImportError, AttributeError):
    from pydantic import Field, root_validator  # type: ignore[no-redef, assignment]

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

        are populated from connection attributes, and cannot be overridden by the user in ``ReadOptions`` to avoid issues.

    .. versionadded:: 0.9.0

    Examples
    --------

    Read options initialization

    .. code:: python

        options = Kafka.ReadOptions(
            include_headers=False,
            minPartitions=50,
        )
    """

    include_headers: bool = Field(default=False, alias="includeHeaders")
    """
    If ``True``, add ``headers`` column to output DataFrame.

    If ``False``, column will not be added.
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

        The set of supported options depends on connector version. See link above.

    .. warning::

        Options:
            * ``kafka.*``
            * ``topic``

        are populated from connection attributes, and cannot be overridden by the user in ``WriteOptions`` to avoid issues.

    .. versionadded:: 0.9.0

    Examples
    --------

    Write options initialization

    .. code:: python

        options = Kafka.WriteOptions(
            if_exists="append",
            include_headers=True,
        )
    """

    if_exists: KafkaTopicExistBehaviorKafka = Field(default=KafkaTopicExistBehaviorKafka.APPEND)
    """Behavior of writing data into existing topic.

    Same as ``df.write.mode(...)``.

    Possible values:
        * ``append`` (default) - Adds new objects into existing topic.
        * ``error`` - Raises an error if topic already exists.
    """

    include_headers: bool = Field(default=False, alias="includeHeaders")
    """
    If ``True``, ``headers`` column from dataframe can be written to Kafka (requires Kafka 2.0+).

    If ``False`` and dataframe contains ``headers`` column, an exception will be raised.
    """

    class Config:
        prohibited_options = PROHIBITED_OPTIONS | KNOWN_READ_OPTIONS
        known_options: frozenset[str] = frozenset()
        extra = "allow"

    @root_validator(pre=True)
    def _mode_is_restricted(cls, values):
        if "mode" in values:
            raise ValueError("Parameter `mode` is not allowed. Please use `if_exists` parameter instead.")
        return values
