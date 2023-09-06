.. _mssql-read:

Reading from MSSQL
==================

There are 2 ways of distributed data reading from MSSQL:

* Using :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with different :ref:`strategy`
* Using :obj:`MSSQL.sql <onetl.connection.db_connection.mssql.connection.MSSQL.sql>`

Both methods accept :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`

.. currentmodule:: onetl.connection.db_connection.mssql.connection

.. automethod:: MSSQL.sql

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
