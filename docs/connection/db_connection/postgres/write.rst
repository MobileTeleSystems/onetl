.. _postgres-write:

Writing to Postgres using ``DBWriter``
========================================

For writing data to Postgres, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

.. warning::

    Please take into account :ref:`postgres-types`

.. warning::

    It is always recommended to create table explicitly using :obj:`Postgres.execute <onetl.connection.db_connection.postgres.connection.Postgres.execute>`
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.

Examples
--------

.. code-block:: python

    from onetl.connection import Postgres
    from onetl.db import DBWriter

    postgres = Postgres(...)

    df = ...  # data is here

    writer = DBWriter(
        connection=postgres,
        target="schema.table",
        options=Postgres.WriteOptions(if_exists="append"),
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
