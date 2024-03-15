.. _mssql-write:

Writing to MSSQL using ``DBWriter``
====================================

For writing data to MSSQL, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

.. warning::

    Please take into account :ref:`mssql-types`

.. warning::

    It is always recommended to create table explicitly using :ref:`MSSQL.execute <mssql-execute>`
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.

Examples
--------

.. code-block:: python

    from onetl.connection import MSSQL
    from onetl.db import DBWriter

    mssql = MSSQL(...)

    df = ...  # data is here

    writer = DBWriter(
        connection=mssql,
        target="schema.table",
        options=MSSQL.WriteOptions(if_exists="append"),
    )

    writer.run(df)

Options
-------

Method above accepts  :obj:`JDBCWriteOptions <onetl.connection.db_connection.jdbc.options.JDBCWriteOptions>`

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCWriteOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
