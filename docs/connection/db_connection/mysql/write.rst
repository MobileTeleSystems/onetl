.. _mysql-write:

Writing to MySQL using ``DBWriter``
========================================

For writing data to MySQL, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

.. warning::

    Please take into account :ref:`mysql-types`

.. warning::

    It is always recommended to create table explicitly using :ref:`MySQL.execute <mysql-execute>`
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

Options
-------

Method above accepts  :obj:`MySQLWriteOptions <onetl.connection.db_connection.mysql.options.MySQLWriteOptions>`

.. currentmodule:: onetl.connection.db_connection.mysql.options

.. autopydantic_model:: MySQLWriteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
