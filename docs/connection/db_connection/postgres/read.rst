.. _postgres-read:

Reading from Postgres
=====================

There are 2 ways of distributed data reading from Postgres:

* Using :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with different :ref:`strategy`
* Using :obj:`Postgres.sql <onetl.connection.db_connection.postgres.connection.Postgres.sql>`

Both methods accept :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`

.. currentmodule:: onetl.connection.db_connection.postgres.connection

.. automethod:: Postgres.sql

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
