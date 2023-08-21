.. _oracle-read:

Reading from Oracle
===================

There are 2 ways of distributed data reading from Oracle:

* Using :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with different :ref:`strategy`
* Using :obj:`Oracle.sql <onetl.connection.db_connection.oracle.connection.Oracle.sql>`

Both methods accept :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`

.. currentmodule:: onetl.connection.db_connection.oracle.connection

.. automethod:: Oracle.sql

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
