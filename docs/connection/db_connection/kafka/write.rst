.. _kafka-write:

Writing to Kafka
================

For writing data to Kafka, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>` with specific options (see below).

.. note::

    Unlike other connection classes, Kafka only accepts dataframe with fixed schema
    (see `documentation <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_):

    .. dropdown:: DataFrame Schema

        .. code:: python

            from pyspark.sql.types import (
                ArrayType,
                BinaryType,
                IntegerType,
                StringType,
                StructField,
                StructType,
            )

            schema = StructType(
                [
                    # mandatory fields:
                    StructField("value", BinaryType(), nullable=True),
                    # optional fields, can be omitted:
                    StructField("key", BinaryType(), nullable=True),
                    StructField("partition", IntegerType(), nullable=True),
                    # this field can be passed only with ``include_headers=True``
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

    You cannot pass dataframe with other column names or types.

.. warning::

    Columns:

    * ``value``
    * ``key``
    * ``headers[*].value``

    can only be string or raw bytes. If they contain values of custom type, these values should be serialized manually.

.. currentmodule:: onetl.connection.db_connection.kafka.options

.. autopydantic_model:: KafkaWriteOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
