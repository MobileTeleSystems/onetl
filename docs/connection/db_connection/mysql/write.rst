.. _mysql-write:

Writing to MySQL using ``DBWriter``
========================================

For writing data to MySQL, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

.. warning::

    Please take into account :ref:`mysql-types`

.. warning::

    It is always recommended to create table explicitly using :obj:`MySQL.execute <onetl.connection.db_connection.mysql.connection.MySQL.execute>`
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.

Examples
--------

.. code-block:: python

    from onetl.connection import MySQL
    from onetl.db import DBWriter

    mysql = MySQL(...)

    df = ...  # data is here

    writer = DBWriter(
        connection=mysql,
        target="schema.table",
        options=MySQL.WriteOptions(
            if_exists="append",
            # ENGINE is required by MySQL
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
