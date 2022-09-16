.. _hive:

Hive connection
===============

.. currentmodule:: onetl.connection.db_connection.hive

.. autosummary::

    Hive
    Hive.WriteOptions

.. autoclass:: Hive
    :members: check, sql, execute

.. currentmodule:: onetl.connection.db_connection.hive.Hive

.. autopydantic_model:: WriteOptions
    :members: mode, format, partition_by, bucket_by, sort_by, compression
    :member-order: bysource
