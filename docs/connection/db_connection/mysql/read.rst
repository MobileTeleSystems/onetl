.. _mysql-read:

Reading from MySQL
==================

There are 2 ways of distributed data reading from MySQL:

* Using :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with different :ref:`strategy`
* Using :obj:`MySQL.sql <onetl.connection.db_connection.mysql.connection.MySQL.sql>`

Both methods accept :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`

.. currentmodule:: onetl.connection.db_connection.mysql.connection

.. automethod:: MySQL.sql

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
