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
    :members: check, sql, fetch, execute

.. currentmodule:: onetl.connection.db_connection.clickhouse.Clickhouse

.. autopydantic_model:: ReadOptions
    :members: fetchsize, partitioning_mode, partition_column, num_partitions, lower_bound, upper_bound, session_init_statement
    :member-order: bysource

.. autopydantic_model:: WriteOptions
    :members: mode, batchsize, isolation_level, query_timeout
    :member-order: bysource

.. autopydantic_model:: JDBCOptions
    :members: query_timeout, fetchsize
    :member-order: bysource
