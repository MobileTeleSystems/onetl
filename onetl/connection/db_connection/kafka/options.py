from onetl.impl import GenericOptions

PROHIBITED_OPTIONS = frozenset(
    (
        "assign",
        "endingOffsets",
        "endingOffsets",
        "endingOffsetsByTimestamp",
        "kafka.*",
        "startingOffsets",
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

        Options ``["assign", "endingOffsets", "endingOffsets", "endingOffsetsByTimestamp", "kafka.*",
        "startingOffsets", "startingOffsets", "startingOffsetsByTimestamp", "startingOffsetsByTimestampStrategy",
        "startingTimestamp", "subscribe", "subscribePattern", "topic"]`` are populated from connection
        attributes, and cannot be set in ``KafkaReadOptions`` class and be overridden by the user to avoid
        duplicating them in the documentation.

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

        Options ``["assign", "endingOffsets", "endingOffsets", "endingOffsetsByTimestamp", "kafka.*",
        "startingOffsets", "startingOffsets", "startingOffsetsByTimestamp", "startingOffsetsByTimestampStrategy",
        "startingTimestamp", "subscribe", "subscribePattern", "topic"]`` are populated from connection
        attributes, and cannot be set in ``KafkaReadOptions`` class and be overridden by the user to avoid
        duplicating them in the documentation.

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
