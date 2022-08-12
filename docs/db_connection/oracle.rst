.. _oracle:

Oracle connection
==================

.. currentmodule:: onetl.connection.db_connection.oracle

.. autosummary::

    Oracle
    Oracle.ReadOptions
    Oracle.WriteOptions
    Oracle.JDBCOptions

.. autoclass:: Oracle
    :members: check, sql, fetch, execute, close

.. currentmodule:: onetl.connection.db_connection.oracle.Oracle

.. autoclass:: ReadOptions
    :members: fetchsize, partition_column, num_partitions, lower_bound, upper_bound, session_init_statement
    :member-order: bysource

.. autoclass:: WriteOptions
    :members: mode, batchsize, isolation_level, query_timeout
    :member-order: bysource

.. autoclass:: JDBCOptions
    :members: query_timeout
    :member-order: bysource
