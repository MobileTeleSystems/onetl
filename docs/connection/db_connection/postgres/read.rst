.. _postgres-read:

Reading from Postgres using ``DBReader``
==========================================

.. warning::

    Please take into account :ref:`postgres-types`

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
* ❌ ``hint`` (is not supported by Postgres)
* ❌ ``df_schema``
* ✅︎ ``options`` (see :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`)

Examples
--------

Snapshot strategy:

.. code-block:: python

    from onetl.connection import Postgres
    from onetl.db import DBReader

    postgres = Postgres(...)

    reader = DBReader(
        connection=postgres,
        source="schema.table",
        columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
        where="key = 'something'",
        options=Postgres.ReadOptions(partition_column="id", num_partitions=10),
    )
    df = reader.run()

Incremental strategy:

.. code-block:: python

    from onetl.connection import Postgres
    from onetl.db import DBReader
    from onetl.strategy import IncrementalStrategy

    postgres = Postgres(...)

    reader = DBReader(
        connection=postgres,
        source="schema.table",
        columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
        where="key = 'something'",
        hwm=DBReader.AutoDetectHWM(name="postgres_hwm", expression="updated_dt"),
        options=Postgres.ReadOptions(partition_column="id", num_partitions=10),
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
