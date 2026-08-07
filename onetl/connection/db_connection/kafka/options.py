# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from enum import Enum

from pydantic import ConfigDict, Field, model_validator

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

    !!! warning

        Options:
            * `assign`
            * `endingOffsets`
            * `endingOffsetsByTimestamp`
            * `kafka.*`
            * `startingOffsets`
            * `startingOffsetsByTimestamp`
            * `startingTimestamp`
            * `subscribe`
            * `subscribePattern`

        are populated from connection attributes,
        and cannot be overridden by the user in `ReadOptions` to avoid issues.

    !!! success "Added in 0.9.0"

    Examples
    --------

    !!! note

        You can pass any value
        [supported by connector](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html),
        even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

        The set of supported options depends on connector version.

    ```python
    from onetl.connection import Kafka

    options = Kafka.ReadOptions(
        includeHeaders=False,
        minPartitions=50,
    )
    ```
    """

    include_headers: bool = Field(default=False, alias="includeHeaders")
    """
    If `True`, add `headers` column to output DataFrame.

    If `False`, column will not be added.
    """
    model_config = ConfigDict(prohibited_options=PROHIBITED_OPTIONS, known_options=KNOWN_READ_OPTIONS, extra="allow")  # type: ignore[typeddict-unknown-key]


class KafkaWriteOptions(GenericOptions):
    """Writing options for Kafka connector.

    !!! warning

        Options:
            * `kafka.*`
            * `topic`

        are populated from connection attributes,
        and cannot be overridden by the user in `WriteOptions` to avoid issues.

    !!! success "Added in 0.9.0"

    Examples
    --------

    !!! note

        You can pass any value
        [supported by connector](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html),
        even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

        The set of supported options depends on connector version.

    ```python
    from onetl.connection import Kafka

    options = Kafka.WriteOptions(
        if_exists="append",
        includeHeaders=True,
    )
    ```
    """

    if_exists: KafkaTopicExistBehaviorKafka = Field(default=KafkaTopicExistBehaviorKafka.APPEND)
    """Behavior of writing data into existing topic.

    Same as `df.write.mode(...)`.

    Possible values:

    * `append` (default) - Adds new objects into existing topic.
    * `error` - Raises an error if topic already exists.
    """

    include_headers: bool = Field(default=False, alias="includeHeaders")
    """
    If `True`, `headers` column from dataframe can be written to Kafka (requires Kafka 2.0+).

    If `False` and dataframe contains `headers` column, an exception will be raised.
    """
    model_config = ConfigDict(  # type: ignore[typeddict-unknown-key]
        prohibited_options=PROHIBITED_OPTIONS | KNOWN_READ_OPTIONS,
        known_options=[],
        extra="allow",
    )

    @model_validator(mode="before")
    @classmethod
    def _mode_is_restricted(cls, values):
        if "mode" in values:
            msg = "Parameter `mode` is not allowed. Please use `if_exists` parameter instead."
            raise ValueError(msg)
        return values
