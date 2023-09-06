.. _kafka-read:

Reading from Kafka
==================

For reading data from Kafka, use :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with specific options (see below).

.. warning::

    Currently, Kafka does not support :ref:`strategy`. You can only read the whole topic, or use ``group.id`` / ``groupIdPrefix`` so Kafka
    will return only messages added to topic since last read.

.. note::

    Unlike other connection classes, Kafka always return dataframe with fixed schema
    (see `documentation <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_):

    .. dropdown:: DataFrame Schema

        .. code:: python

            from pyspark.sql.types import (
                ArrayType,
                BinaryType,
                IntegerType,
                LongType,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            schema = StructType(
                [
                    StructField("value", BinaryType(), nullable=True),
                    StructField("key", BinaryType(), nullable=True),
                    StructField("topic", StringType(), nullable=False),
                    StructField("partition", IntegerType(), nullable=False),
                    StructField("offset", LongType(), nullable=False),
                    StructField("timestamp", TimestampType(), nullable=False),
                    StructField("timestampType", IntegerType(), nullable=False),
                    # this field is returned only with ``include_headers=True``
                    StructField(
                        "headers",
                        ArrayType(
                            StructType(
                                [
                                    StructField("key", StringType(), nullable=False),
                                    StructField("value", BinaryType(), nullable=True),
                                ],
                            ),
                        ),
                        nullable=True,
                    ),
                ],
            )

.. warning::

    Columns:

    * ``value``
    * ``key``
    * ``headers[*].value``

    are always returned as raw bytes. If they contain values of custom type, these values should be deserialized manually.

.. currentmodule:: onetl.connection.db_connection.kafka.options

.. autopydantic_model:: KafkaReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
