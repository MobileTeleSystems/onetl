.. _hdfs:

HDFS connection
===============

.. currentmodule:: onetl.connection.file_connection.hdfs

.. autosummary::

    HDFS
    HDFS.slots

.. autoclass:: HDFS
    :members: __init__, check

.. currentmodule:: onetl.connection.file_connection.hdfs.HDFS

.. autoclass:: slots
    :members: normalize_cluster_name, normalize_namenode_name, get_known_clusters, get_cluster_namenodes, get_current_cluster, get_webhdfs_port, is_namenode_active
    :member-order: bysource
