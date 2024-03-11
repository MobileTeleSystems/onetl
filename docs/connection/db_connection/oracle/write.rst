.. _oracle-write:

Writing to Oracle using ``DBWriter``
====================================

For writing data to Oracle, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

.. warning::

    Please take into account :ref:`oracle-types`

.. warning::

    It is always recommended to create table explicitly using :obj:`Oracle.execute <onetl.connection.db_connection.oracle.connection.Oracle.execute>`
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.

Examples
--------

.. code-block:: python

    from onetl.connection import Oracle
    from onetl.db import DBWriter

    oracle = Oracle(...)

    df = ...  # data is here

    writer = DBWriter(
        connection=oracle,
        target="schema.table",
        options=Oracle.WriteOptions(if_exists="append"),
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
