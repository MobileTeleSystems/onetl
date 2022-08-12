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
    :members: check, sql, fetch, execute, close

.. currentmodule:: onetl.connection.db_connection.teradata.Teradata

.. autoclass:: ReadOptions
    :members: fetchsize, partition_column, num_partitions, lower_bound, upper_bound, session_init_statement
    :member-order: bysource

.. autoclass:: WriteOptions
    :members: mode, batchsize, isolation_level, query_timeout
    :member-order: bysource

.. autoclass:: JDBCOptions
    :members: query_timeout
    :member-order: bysource
