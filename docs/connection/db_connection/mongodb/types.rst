.. _mongodb-types:

MongoDB <-> Spark type mapping
==============================

.. note::

    The results below are valid for Spark 3.5.5, and may differ on other Spark versions.

Type detection & casting
------------------------

Spark's DataFrames always have a ``schema`` which is a list of fields with corresponding Spark types. All operations on a field are performed using field type.

MongoDB is, by design, __schemaless__. So there are 2 ways how this can be handled:

* User provides DataFrame schema explicitly:

  .. dropdown:: See example

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

        df_schema = StructType(
            [
                StructField("_id", StringType()),
                StructField("some", StringType()),
                StructField(
                    "field",
                    StructType(
                        [
                            StructField("nested", IntegerType()),
                        ]
                    ),
                ),
            ]
        )

        reader = DBReader(
            connection=mongodb,
            source="some_collection",
            df_schema=df_schema,
        )
        df = reader.run()

        # or

        df = mongodb.pipeline(
            collection="some_collection",
            df_schema=df_schema,
        )

* Rely on MongoDB connector schema infer:

  .. code-block:: python

    df = mongodb.pipeline(collection="some_collection")

  In this case MongoDB connector read a sample of collection documents, and build DataFrame schema based on document fields and values.

It is highly recommended to pass ``df_schema`` explicitly, to avoid type conversion issues.

References
~~~~~~~~~~

Here you can find source code with type conversions:

* `MongoDB -> Spark <https://github.com/mongodb/mongo-spark/blob/r10.4.1/src/main/java/com/mongodb/spark/sql/connector/schema/InferSchema.java#L214-L260>`_
* `Spark -> MongoDB <https://github.com/mongodb/mongo-spark/blob/r10.4.1/src/main/java/com/mongodb/spark/sql/connector/schema/RowToBsonDocumentConverter.java#L157-L260>`_

Supported types
---------------

See `official documentation <https://www.mongodb.com/docs/manual/reference/bson-types/>`_

Numeric types
~~~~~~~~~~~~~

+---------------------+-----------------------------+----------------------+
| MongoDB type (read) | Spark type                  | MongoDB type (write) |
+=====================+=============================+======================+
| ``Decimal128``      | ``DecimalType(P=34, S=32)`` | ``Decimal128``       |
+---------------------+-----------------------------+----------------------+
| ``-``               | ``FloatType()``             | ``Double``           |
+---------------------+-----------------------------+                      |
| ``Double``          | ``DoubleType()``            |                      |
+---------------------+-----------------------------+----------------------+
| ``-``               | ``ByteType()``              | ``Int32``            |
+---------------------+-----------------------------+                      |
| ``-``               | ``ShortType()``             |                      |
+---------------------+-----------------------------+                      |
| ``Int32``           | ``IntegerType()``           |                      |
+---------------------+-----------------------------+----------------------+
| ``Int64``           | ``LongType()``              | ``Int64``            |
+---------------------+-----------------------------+----------------------+

Temporal types
~~~~~~~~~~~~~~

+------------------------+-----------------------------------+-------------------------+
| MongoDB type (read)    | Spark type                        | MongoDB type (write)    |
+========================+===================================+=========================+
| ``-``                  | ``DateType()``, days              | ``Date``, milliseconds  |
+------------------------+-----------------------------------+-------------------------+
| ``Date``, milliseconds | ``TimestampType()``, microseconds | ``Date``, milliseconds, |
|                        |                                   | **precision loss** [2]_ |
+------------------------+-----------------------------------+-------------------------+
| ``Timestamp``, seconds | ``TimestampType()``, microseconds | ``Date``, milliseconds  |
+------------------------+-----------------------------------+-------------------------+
| ``-``                  | ``TimestampNTZType()``            | unsupported             |
+------------------------+-----------------------------------+                         |
| ``-``                  | ``DayTimeIntervalType()``         |                         |
+------------------------+-----------------------------------+-------------------------+

.. warning::

    Note that types in MongoDB and Spark have different value ranges:

    +---------------+--------------------------------+--------------------------------+---------------------+--------------------------------+--------------------------------+
    | MongoDB type  | Min value                      | Max value                      | Spark type          | Min value                      | Max value                      |
    +===============+================================+================================+=====================+================================+================================+
    | ``Date``      | -290 million years             | 290 million years              | ``TimestampType()`` | ``0001-01-01 00:00:00.000000`` | ``9999-12-31 23:59:59.999999`` |
    +---------------+--------------------------------+--------------------------------+                     |                                |                                |
    | ``Timestamp`` | ``1970-01-01 00:00:00``        | ``2106-02-07 09:28:16``        |                     |                                |                                |
    +---------------+--------------------------------+--------------------------------+---------------------+--------------------------------+--------------------------------+

    So not all values can be read from MongoDB to Spark, and can written from Spark DataFrame to MongoDB.

    References:
        * `MongoDB Date type documentation <https://www.mongodb.com/docs/manual/reference/bson-types/#date>`_
        * `MongoDB Timestamp documentation <https://www.mongodb.com/docs/manual/reference/bson-types/#timestamps>`_
        * `Spark DateType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html>`_
        * `Spark TimestampType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html>`_

.. [2]
    MongoDB ``Date`` type has precision up to milliseconds (``23:59:59.999``).
    Inserting data with microsecond precision (``23:59:59.999999``)
    will lead to **throwing away microseconds**.

String types
~~~~~~~~~~~~~

Note: fields of deprecated MongoDB type ``Symbol`` are excluded during read.

+---------------------+------------------+----------------------+
| MongoDB type (read) | Spark type       | MongoDB type (write) |
+=====================+==================+======================+
| ``String``          | ``StringType()`` | ``String``           |
+---------------------+                  |                      |
| ``Code``            |                  |                      |
+---------------------+                  |                      |
| ``RegExp``          |                  |                      |
+---------------------+------------------+----------------------+

Binary types
~~~~~~~~~~~~

+---------------------+-------------------+----------------------+
| MongoDB type (read) | Spark type        | MongoDB type (write) |
+=====================+===================+======================+
| ``Boolean``         | ``BooleanType()`` | ``Boolean``          |
+---------------------+-------------------+----------------------+
| ``Binary``          | ``BinaryType()``  | ``Binary``           |
+---------------------+-------------------+----------------------+

Struct types
~~~~~~~~~~~~

+---------------------+-----------------------+----------------------+
| MongoDB type (read) | Spark type            | MongoDB type (write) |
+=====================+=======================+======================+
| ``Array[T]``        | ``ArrayType(T)``      | ``Array[T]``         |
+---------------------+-----------------------+----------------------+
| ``Object[...]``     | ``StructType([...])`` | ``Object[...]``      |
+---------------------+-----------------------+                      |
| ``-``               | ``MapType(...)``      |                      |
+---------------------+-----------------------+----------------------+

Special types
~~~~~~~~~~~~~

+---------------------+---------------------------------------------------------+---------------------------------------+
| MongoDB type (read) | Spark type                                              | MongoDB type (write)                  |
+=====================+=========================================================+=======================================+
| ``ObjectId``        | ``StringType()``                                        | ``String``                            |
+---------------------+                                                         |                                       |
| ``MaxKey``          |                                                         |                                       |
+---------------------+                                                         |                                       |
| ``MinKey``          |                                                         |                                       |
+---------------------+---------------------------------------------------------+---------------------------------------+
| ``Null``            | ``NullType()``                                          | ``Null``                              |
+---------------------+                                                         |                                       |
| ``Undefined``       |                                                         |                                       |
+---------------------+---------------------------------------------------------+---------------------------------------+
| ``DBRef``           | ``StructType([$ref: StringType(), $id: StringType()])`` | ``Object[$ref: String, $id: String]`` |
+---------------------+---------------------------------------------------------+---------------------------------------+

Explicit type cast
------------------

``DBReader``
~~~~~~~~~~~~

Currently it is not possible to cast field types using ``DBReader``. But this can be done using ``MongoDB.pipeline``.

``MongoDB.pipeline``
~~~~~~~~~~~~~~~~~~~~

You can use ``$project`` aggregation to cast field types:

.. code-block:: python

    from pyspark.sql.types import IntegerType, StructField, StructType

    from onetl.connection import MongoDB
    from onetl.db import DBReader

    mongodb = MongoDB(...)

    df = mongodb.pipeline(
        collection="my_collection",
        pipeline=[
            {
                "$project": {
                    # convert unsupported_field to string
                    "unsupported_field_str": {
                        "$convert": {
                            "input": "$unsupported_field",
                            "to": "string",
                        },
                    },
                    # skip unsupported_field from result
                    "unsupported_field": 0,
                }
            }
        ],
    )

    # cast field content to proper Spark type
    df = df.select(
        df.id,
        df.supported_field,
        # explicit cast
        df.unsupported_field_str.cast("integer").alias("parsed_integer"),
    )

``DBWriter``
~~~~~~~~~~~~

Convert dataframe field to string on Spark side, and then write it to MongoDB:

.. code:: python


    df = df.select(
        df.id,
        df.unsupported_field.cast("string").alias("array_field_json"),
    )

    writer.run(df)
