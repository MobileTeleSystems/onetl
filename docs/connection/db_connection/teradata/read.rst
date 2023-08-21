.. _teradata-read:

Reading from Teradata
=====================

There are 2 ways of distributed data reading from Teradata:

* Using :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with different :ref:`strategy`
* Using :obj:`Teradata.sql <onetl.connection.db_connection.teradata.connection.Teradata.sql>`

Both methods accept :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`

.. currentmodule:: onetl.connection.db_connection.teradata.connection

.. automethod:: Teradata.sql

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
