.. _mongodb-read:

Reading from MongoDB using ``DBReader``
=======================================

.. warning::

    Please take into account :ref:`mongodb-types`

:obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` supports :ref:`strategy` for incremental data reading,
but does not support custom pipelines, e.g. aggregation.

Supported DBReader features
---------------------------

* ❌ ``columns`` (for now, all document fields are read)
* ✅︎ ``where`` (passed to ``{"$match": ...}`` aggregation pipeline)
* ✅︎ ``hwm``, supported strategies:
* * ✅︎ :ref:`snapshot-strategy`
* * ✅︎ :ref:`incremental-strategy`
* * ✅︎ :ref:`snapshot-batch-strategy`
* * ✅︎ :ref:`incremental-batch-strategy`
* * Note that ``expression`` field of HWM can only be a field name, not a custom expression
* ✅︎ ``hint`` (see `official documentation <https://www.mongodb.com/docs/v5.0/reference/operator/meta/hint/>`_)
* ✅︎ ``df_schema`` (mandatory)
* ✅︎ ``options`` (see :obj:`MongoDBReadOptions <onetl.connection.db_connection.mongodb.options.MongoDBReadOptions>`)

Examples
--------

Snapshot strategy:

.. code-block:: python

    from onetl.connection import MongoDB
    from onetl.db import DBReader

    from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        StringType,
        TimestampType,
    )

    mongodb = MongoDB(...)

    # mandatory
    df_schema = StructType(
        [
            StructField("_id", StringType()),
            StructField("some", StringType()),
            StructField(
                "field",
                StructType(
                    [
                        StructField("nested", IntegerType()),
                    ],
                ),
            ),
            StructField("updated_dt", TimestampType()),
        ]
    )

    reader = DBReader(
        connection=mongodb,
        source="some_collection",
        df_schema=df_schema,
        where={"field": {"$eq": 123}},
        hint={"field": 1},
        options=MongoDBReadOptions(batchSize=10000),
    )
    df = reader.run()

Incremental strategy:

.. code-block:: python

    from onetl.connection import MongoDB
    from onetl.db import DBReader
    from onetl.strategy import IncrementalStrategy

    from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        StringType,
        TimestampType,
    )

    mongodb = MongoDB(...)

    # mandatory
    df_schema = StructType(
        [
            StructField("_id", StringType()),
            StructField("some", StringType()),
            StructField(
                "field",
                StructType(
                    [
                        StructField("nested", IntegerType()),
                    ],
                ),
            ),
            StructField("updated_dt", TimestampType()),
        ]
    )

    reader = DBReader(
        connection=mongodb,
        source="some_collection",
        df_schema=df_schema,
        where={"field": {"$eq": 123}},
        hint={"field": 1},
        hwm=DBReader.AutoDetectHWM(name="mongodb_hwm", expression="updated_dt"),
        options=MongoDBReadOptions(batchSize=10000),
    )

    with IncrementalStrategy():
        df = reader.run()

Read options
------------

.. currentmodule:: onetl.connection.db_connection.mongodb.options

.. autopydantic_model:: MongoDBReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
