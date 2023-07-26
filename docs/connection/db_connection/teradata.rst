.. _teradata:

Teradata connection
====================

.. currentmodule:: onetl.connection.db_connection.teradata

.. autosummary::

    Teradata
    Teradata.ReadOptions
    Teradata.WriteOptions
    Teradata.JDBCOptions

.. autoclass:: Teradata
    :members: get_packages, check, sql, fetch, execute, close

.. currentmodule:: onetl.connection.db_connection.teradata.Teradata

.. autopydantic_model:: ReadOptions
    :members: fetchsize, partitioning_mode, partition_column, num_partitions, lower_bound, upper_bound, session_init_statement
    :member-order: bysource

.. autopydantic_model:: WriteOptions
    :members: mode, batchsize, isolation_level, query_timeout
    :member-order: bysource

.. autopydantic_model:: JDBCOptions
    :members: query_timeout, fetchsize
    :member-order: bysource
