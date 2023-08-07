.. _hive:

Hive connection
===============

.. currentmodule:: onetl.connection.db_connection.hive

.. autosummary::

    Hive
    Hive.WriteOptions
    Hive.Slots

.. autoclass:: Hive
    :members: get_current, check, sql, execute
    :member-order: bysource

.. currentmodule:: onetl.connection.db_connection.hive.Hive

.. autopydantic_model:: WriteOptions
    :members: mode, format, partition_by, bucket_by, sort_by, compression
    :member-order: bysource

.. autoclass:: Slots
    :members: normalize_cluster_name, get_known_clusters, get_current_cluster
    :member-order: bysource
