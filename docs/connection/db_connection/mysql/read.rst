.. _mysql-read:

Reading from MySQL using ``DBReader``
=====================================

:obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` supports :ref:`strategy` for incremental data reading,
but does not support custom queries, like ``JOIN``.

.. warning::

    Please take into account :ref:`mysql-types`

Supported DBReader features
---------------------------

* ✅︎ ``columns``
* ✅︎ ``where``
* ✅︎ ``hwm``, supported strategies:
* * ✅︎ :ref:`snapshot-strategy`
* * ✅︎ :ref:`incremental-strategy`
* * ✅︎ :ref:`snapshot-batch-strategy`
* * ✅︎ :ref:`incremental-batch-strategy`
* ✅︎ ``hint`` (see `official documentation <https://dev.mysql.com/doc/refman/en/optimizer-hints.html>`_)
* ❌ ``df_schema``
* ✅︎ ``options`` (see :obj:`MySQL.ReadOptions <onetl.connection.db_connection.mysql.options.MySQLReadOptions>`)

Examples
--------

Snapshot strategy:

.. code-block:: python

    from onetl.connection import MySQL
    from onetl.db import DBReader

    mysql = MySQL(...)

    reader = DBReader(
        connection=mysql,
        source="schema.table",
        columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
        where="key = 'something'",
        hint="SKIP_SCAN(schema.table key_index)",
        options=MySQL.ReadOptions(partitionColumn="id", numPartitions=10),
    )
    df = reader.run()

Incremental strategy:

.. code-block:: python

    from onetl.connection import MySQL
    from onetl.db import DBReader
    from onetl.strategy import IncrementalStrategy

    mysql = MySQL(...)

    reader = DBReader(
        connection=mysql,
        source="schema.table",
        columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
        where="key = 'something'",
        hint="SKIP_SCAN(schema.table key_index)",
        hwm=DBReader.AutoDetectHWM(name="mysql_hwm", expression="updated_dt"),
        options=MySQL.ReadOptions(partitionColumn="id", numPartitions=10),
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
Especially if there are indexes for columns used in ``where`` clause.

Options
-------

.. currentmodule:: onetl.connection.db_connection.mysql.options

.. autopydantic_model:: MySQLReadOptions
    :inherited-members: GenericOptions
    :member-order: bysource
