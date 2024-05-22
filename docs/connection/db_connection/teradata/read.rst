.. _teradata-read:

Reading from Teradata using ``DBReader``
========================================

:obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` supports :ref:`strategy` for incremental data reading,
but does not support custom queries, like ``JOIN``.

Supported DBReader features
---------------------------

* ✅︎ ``columns``
* ✅︎ ``where``
* ✅︎ ``hwm``, supported strategies:
* * ✅︎ :ref:`snapshot-strategy`
* * ✅︎ :ref:`incremental-strategy`
* * ✅︎ :ref:`snapshot-batch-strategy`
* * ✅︎ :ref:`incremental-batch-strategy`
* ❌ ``hint`` (is not supported by Teradata)
* ❌ ``df_schema``
* ✅︎ ``options`` (see :obj:`TeradataReadOptions <onetl.connection.db_connection.teradata.options.TeradataReadOptions>`)

Examples
--------

Snapshot strategy:

.. code-block:: python

    from onetl.connection import Teradata
    from onetl.db import DBReader

    teradata = Teradata(...)

    reader = DBReader(
        connection=teradata,
        source="database.table",
        columns=["id", "key", "CAST(value AS VARCHAR) value", "updated_dt"],
        where="key = 'something'",
        options=Teradata.ReadOptions(
            partition_column="id",
            num_partitions=10,
            partitioning_mode="hash",
        ),
    )
    df = reader.run()

Incremental strategy:

.. code-block:: python

    from onetl.connection import Teradata
    from onetl.db import DBReader
    from onetl.strategy import IncrementalStrategy

    teradata = Teradata(...)

    reader = DBReader(
        connection=teradata,
        source="database.table",
        columns=["id", "key", "CAST(value AS VARCHAR) value", "updated_dt"],
        where="key = 'something'",
        hwm=DBReader.AutoDetectHWM(name="teradata_hwm", expression="updated_dt"),
        options=Teradata.ReadOptions(
            partition_column="id",
            num_partitions=10,
            partitioning_mode="hash",
        ),
    )

    with IncrementalStrategy():
        df = reader.run()

Recommendations
---------------

Select only required columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of passing ``"*"`` in ``DBReader(columns=[...])`` prefer passing exact column names. This reduces the amount of data passed from Teradata to Spark.

Pay attention to ``where`` value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of filtering data on Spark side using ``df.filter(df.column == 'value')`` pass proper ``DBReader(where="column = 'value'")`` clause.
This both reduces the amount of data send from Teradata to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in ``where`` clause.

Read data in parallel
~~~~~~~~~~~~~~~~~~~~~

``DBReader`` can read data in multiple parallel connections by passing ``Teradata.ReadOptions(num_partitions=..., partition_column=...)``.

In the example above, Spark opens 10 parallel connections, and data is evenly distributed between all these connections using expression
``HASHAMP(HASHBUCKET(HASHROW({partition_column}))) MOD {num_partitions}``.
This allows sending each Spark worker only some piece of data, reducing resource consumption.
``partition_column`` here can be table column of any type.

It is also possible to use ``partitioning_mode="mod"`` or ``partitioning_mode="range"``, but in this case
``partition_column`` have to be an integer, should not contain ``NULL``, and values to be uniformly distributed.
It is also less performant than ``partitioning_mode="hash"`` due to Teradata ``HASHAMP`` implementation.

Do **NOT** use ``TYPE=FASTEXPORT``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Teradata supports several `different connection types <https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABFGFAF>`_:
    * ``TYPE=DEFAULT`` - perform plain ``SELECT`` queries
    * ``TYPE=FASTEXPORT`` - uses special FastExport protocol for select queries

But ``TYPE=FASTEXPORT`` uses exclusive lock on the source table, so it is impossible to use multiple Spark workers parallel data read.
This leads to sending all the data to just one Spark worker, which is slow and takes a lot of RAM.

Prefer using ``partitioning_mode="hash"`` from example above.

Options
-------

.. currentmodule:: onetl.connection.db_connection.teradata.options

.. autopydantic_model:: TeradataReadOptions
    :inherited-members: GenericOptions
    :member-order: bysource
