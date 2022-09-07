.. _clickhouse:

Clickhouse connection
=====================

.. currentmodule:: onetl.connection.db_connection.clickhouse

.. autosummary::

    Clickhouse
    Clickhouse.ReadOptions
    Clickhouse.WriteOptions
    Clickhouse.JDBCOptions

.. autoclass:: Clickhouse
    :members: __init__, check, sql, fetch, execute

.. currentmodule:: onetl.connection.db_connection.clickhouse.Clickhouse

.. autoclass:: ReadOptions
    :members: fetchsize, partitioning_mode, partition_column, num_partitions, lower_bound, upper_bound, session_init_statement
    :member-order: bysource

.. autoclass:: WriteOptions
    :members: mode, batchsize, isolation_level, query_timeout
    :member-order: bysource

.. autoclass:: JDBCOptions
    :members: query_timeout, fetchsize
    :member-order: bysource
