.. _greenplum:

Greenplum connection
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
    and then read the data from this table using :obj:`onetl.core.db_reader.db_reader.DBReader`.

    In this case data will be read directly from segment nodes in a distributed way

.. autoclass:: Greenplum
    :members: check, fetch, execute, close

.. currentmodule:: onetl.connection.db_connection.greenplum.Greenplum

.. autoclass:: ReadOptions
    :members: partition_column, num_partitions
    :member-order: bysource

.. autoclass:: WriteOptions
    :members: mode
    :member-order: bysource

.. autoclass:: JDBCOptions
    :members: query_timeout, fetchsize
    :member-order: bysource
