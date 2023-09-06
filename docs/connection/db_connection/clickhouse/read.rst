.. _clickhouse-read:

Reading from Clickhouse
=======================

There are 2 ways of distributed data reading from Clickhouse:

* Using :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with different :ref:`strategy`
* Using :obj:`Clickhouse.sql <onetl.connection.db_connection.clickhouse.connection.Clickhouse.sql>`

Both methods accept :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`

.. currentmodule:: onetl.connection.db_connection.clickhouse.connection

.. automethod:: Clickhouse.sql

.. currentmodule:: onetl.connection.db_connection.jdbc_connection.options

.. autopydantic_model:: JDBCReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
