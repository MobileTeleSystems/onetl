.. _greenplum:

Greenplum connector
====================

.. currentmodule:: onetl.connection.db_connection.greenplum

.. autosummary::

    Greenplum
    Greenplum.ReadOptions
    Greenplum.WriteOptions
    Greenplum.JDBCOptions

.. note::

    Unlike JDBC connectors, *Greenplum connector for Spark* does not support
    executing **custom** SQL queries using ``.sql`` method, because this leads to sending
    the result through *master* node which is really bad for cluster performance.

    To make distributed queries like ``JOIN`` **on Greenplum side**, you should create a temporary table,
    populate it with the data you need (using ``.execute`` method to call ``INSERT INTO ... AS SELECT ...``),
    and then read the data from this table using :obj:`onetl.db.db_reader.db_reader.DBReader`.

    In this case data will be read directly from segment nodes in a distributed way

.. autoclass:: Greenplum
    :members: get_packages, check, fetch, execute, close

.. currentmodule:: onetl.connection.db_connection.greenplum.Greenplum

.. autopydantic_model:: ReadOptions
    :members: partition_column, num_partitions
    :member-order: bysource

.. autopydantic_model:: WriteOptions
    :members: mode
    :member-order: bysource

.. autopydantic_model:: JDBCOptions
    :members: query_timeout, fetchsize
    :member-order: bysource
