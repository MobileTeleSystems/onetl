.. _mssql-read:

Reading from MSSQL using ``DBReader``
======================================

.. warning::

    Please take into account :ref:`mssql-types`

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
* ❌ ``hint`` (MSSQL does support hints, but DBReader not, at least for now)
* ❌ ``df_schema``
* ✅︎ ``options`` (see :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`)

Examples
--------

Snapshot strategy:

.. code-block:: python

    from onetl.connection import MSSQL
    from onetl.db import DBReader

    mssql = MSSQL(...)

    reader = DBReader(
        connection=mssql,
        source="schema.table",
        columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
        where="key = 'something'",
        options=MSSQL.ReadOptions(partition_column="id", num_partitions=10),
    )
    df = reader.run()

Incremental strategy:

.. code-block:: python

    from onetl.connection import MSSQL
    from onetl.db import DBReader
    from onetl.strategy import IncrementalStrategy

    mssql = MSSQL(...)

    reader = DBReader(
        connection=mssql,
        source="schema.table",
        columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
        where="key = 'something'",
        hwm=DBReader.AutoDetectHWM(name="mssql_hwm", expression="updated_dt"),
        options=MSSQL.ReadOptions(partition_column="id", num_partitions=10),
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
