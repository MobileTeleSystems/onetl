.. _postgres:

Postgres connection
====================

.. currentmodule:: onetl.connection.db_connection.postgres

.. autosummary::

    Postgres
    Postgres.ReadOptions
    Postgres.WriteOptions
    Postgres.JDBCOptions

.. autoclass:: Postgres
    :members: get_packages, check, sql, fetch, execute, close

.. currentmodule:: onetl.connection.db_connection.postgres.Postgres

.. autopydantic_model:: ReadOptions
    :members: fetchsize, partitioning_mode, partition_column, num_partitions, lower_bound, upper_bound, session_init_statement
    :member-order: bysource

.. autopydantic_model:: WriteOptions
    :members: mode, batchsize, isolation_level, query_timeout
    :member-order: bysource

.. autopydantic_model:: JDBCOptions
    :members: query_timeout, fetchsize
    :member-order: bysource
