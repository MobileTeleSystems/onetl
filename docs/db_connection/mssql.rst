.. _mssql:

MSSQL connection
================

.. currentmodule:: onetl.connection.db_connection.mssql

.. autosummary::

    MSSQL
    MSSQL.ReadOptions
    MSSQL.WriteOptions
    MSSQL.JDBCOptions

.. autoclass:: MSSQL
    :members: check, sql, fetch, execute, close

.. currentmodule:: onetl.connection.db_connection.mssql.MSSQL

.. autoclass:: ReadOptions
    :members: fetchsize, partitioning_mode, partition_column, num_partitions, lower_bound, upper_bound, session_init_statement
    :member-order: bysource

.. autoclass:: WriteOptions
    :members: mode, batchsize, isolation_level, query_timeout
    :member-order: bysource

.. autoclass:: JDBCOptions
    :members: query_timeout, fetchsize
    :member-order: bysource
