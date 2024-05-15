.. _oracle-read:

Reading from Oracle using ``DBReader``
======================================

:obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` supports :ref:`strategy` for incremental data reading,
but does not support custom queries, like ``JOIN``.

.. warning::

    Please take into account :ref:`oracle-types`

Supported DBReader features
---------------------------

* ✅︎ ``columns``
* ✅︎ ``where``
* ✅︎ ``hwm``, supported strategies:
* * ✅︎ :ref:`snapshot-strategy`
* * ✅︎ :ref:`incremental-strategy`
* * ✅︎ :ref:`snapshot-batch-strategy`
* * ✅︎ :ref:`incremental-batch-strategy`
* ✅︎ ``hint`` (see `official documentation <https://docs.oracle.com/cd/B10500_01/server.920/a96533/hintsref.htm>`_)
* ❌ ``df_schema``
* ✅︎ ``options`` (see :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`)

Examples
--------

Snapshot strategy:

.. code-block:: python

    from onetl.connection import Oracle
    from onetl.db import DBReader

    oracle = Oracle(...)

    reader = DBReader(
        connection=oracle,
        source="schema.table",
        columns=["id", "key", "CAST(value AS VARCHAR2(4000)) value", "updated_dt"],
        where="key = 'something'",
        hint="INDEX(schema.table key_index)",
        options=Oracle.ReadOptions(partition_column="id", num_partitions=10),
    )
    df = reader.run()

Incremental strategy:

.. code-block:: python

    from onetl.connection import Oracle
    from onetl.db import DBReader
    from onetl.strategy import IncrementalStrategy

    oracle = Oracle(...)

    reader = DBReader(
        connection=oracle,
        source="schema.table",
        columns=["id", "key", "CAST(value AS VARCHAR2(4000)) value", "updated_dt"],
        where="key = 'something'",
        hint="INDEX(schema.table key_index)",
        hwm=DBReader.AutoDetectHWM(name="oracle_hwm", expression="updated_dt"),
        options=Oracle.ReadOptions(partition_column="id", num_partitions=10),
    )

    with IncrementalStrategy():
        df = reader.run()

Recommendations
---------------

Select only required columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of passing ``"*"`` in ``DBReader(columns=[...])`` prefer passing exact column names. This reduces the amount of data passed from Oracle to Spark.

Pay attention to ``where`` value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of filtering data on Spark side using ``df.filter(df.column == 'value')`` pass proper ``DBReader(where="column = 'value'")`` clause.
This both reduces the amount of data send from Oracle to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in ``where`` clause.

Options
-------

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false

.. autoclass:: JDBCSQLOptions
