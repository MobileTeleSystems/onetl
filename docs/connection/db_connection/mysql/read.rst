.. _mysql-read:

Reading from MySQL using ``DBReader``
=====================================

.. warning::

    Please take into account :ref:`mysql-types`

:obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` supports :ref:`strategy` for incremental data reading,
but does not support custom queries, like JOINs.

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
* ✅︎ ``options`` (see :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`)

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
        options=MySQL.ReadOptions(partition_column="id", num_partitions=10),
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
        options=MySQL.ReadOptions(partition_column="id", num_partitions=10),
    )

    with IncrementalStrategy():
        df = reader.run()

Read options
------------

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
