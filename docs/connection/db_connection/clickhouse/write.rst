.. _clickhouse-write:

Writing to Clickhouse using ``DBWriter``
========================================

.. warning::

    Please take into account :ref:`clickhouse-types`

.. warning::

    It is always recommended to create table explicitly using :obj:`Clickhouse.execute <onetl.connection.db_connection.clickhouse.connection.Clickhouse.execute>`
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.

For writing data to Clickhouse, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

Examples
--------

.. code-block:: python

    from onetl.connection import Clickhouse
    from onetl.db import DBWriter

    clickhouse = Clickhouse(...)

    df = ...  # data is here

    writer = DBWriter(
        connection=clickhouse,
        target="schema.table",
        options=Clickhouse.WriteOptions(
            if_exists="replace_entire_table",
            createTableOptions="ENGINE = MergeTree() ORDER BY id",
        ),
    )

    writer.run(df)


Write options
-------------

Method above accepts  :obj:`JDBCWriteOptions <onetl.connection.db_connection.jdbc.options.JDBCWriteOptions>`

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCWriteOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
